/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.bench;

import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compares Vamana graph parameters: R=48/Lbuild=128 (baseline) vs R=32/Lbuild=75 (candidate).
 *
 * <p>Decision rule: if recall@10 drops less than 1% with R=32 and build is noticeably
 * faster, accept R=32. Otherwise keep R=48.</p>
 *
 * <p>Context: previous rollback to R=48 was based on queryDetailed taking 15-20 min,
 * which was almost certainly caused by missing sidecar (collectDetailedFromCandidates
 * = full scan), not graph quality. This test settles the question with data.</p>
 *
 * <p>Run: {@code ./gradlew :resonance-core:test --tests "*.GraphParamsBenchmark" -Ppreset=mid}</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("benchmark")
@Timeout(value = 60, unit = TimeUnit.MINUTES)
class GraphParamsBenchmark {

    private static final long DATA_SEED = 271828L;
    private static final long QUERY_SEED = 161803L;

    private static final int DIM = Integer.getInteger("resonance.pattern.len", 1536);
    private static final int N = Integer.getInteger("bench.N", 5_000);
    private static final int NUM_QUERIES = Integer.getInteger("bench.queries", 200);
    private static final int TOP_K = 10;
    private static final int WARMUP = 20;

    @TempDir
    Path tempDir;

    private StoreRuntimeServices runtime;
    private ResonanceKernel kernel;

    private WavePattern[] queries;
    private String[][] groundTruthIds;

    @BeforeAll
    void setup() {
        runtime = StoreRuntimeServices.fromSystemProperties();
        kernel = new JavaKernel();
    }

    @AfterAll
    void teardown() {
        if (runtime != null) runtime.close();
    }

    @Test
    @DisplayName("R=48/Lbuild=128 vs R=32/Lbuild=75")
    void compareGraphParams() {
        int nProbe = Integer.getInteger("resonance.index.l1.nprobe", 8);

        System.out.println();
        System.out.println("=".repeat(80));
        System.out.println("Vamana Graph Parameters Comparison");
        System.out.printf("  N=%d, dim=%d, queries=%d, topK=%d, nProbe=%d%n",
                N, DIM, NUM_QUERIES, TOP_K, nProbe);
        System.out.printf("  heap: %d MB, CPUs: %d%n",
                Runtime.getRuntime().maxMemory() / (1024 * 1024),
                Runtime.getRuntime().availableProcessors());
        System.out.println("=".repeat(80));

        queries = new WavePattern[NUM_QUERIES];
        Random qRng = new Random(QUERY_SEED);
        for (int i = 0; i < NUM_QUERIES; i++) {
            queries[i] = randomPattern(qRng, DIM);
        }

        computeGroundTruth();

        Result baseline = runConfig("baseline", 48, 128);
        Result candidate = runConfig("candidate", 32, 75);

        System.out.println();
        System.out.println("┌──────────────────┬─────────────────┬─────────────────┬──────────┐");
        System.out.println("│ Metric           │ R=48/Lb=128     │ R=32/Lb=75      │ Delta    │");
        System.out.println("├──────────────────┼─────────────────┼─────────────────┼──────────┤");
        System.out.printf( "│ recall@1         │ %.4f          │ %.4f          │ %+.4f  │%n",
                baseline.recall1, candidate.recall1, candidate.recall1 - baseline.recall1);
        System.out.printf( "│ recall@10        │ %.4f          │ %.4f          │ %+.4f  │%n",
                baseline.recall10, candidate.recall10, candidate.recall10 - baseline.recall10);
        System.out.printf( "│ p50 latency (ms) │ %7.1f         │ %7.1f         │ %+.1f    │%n",
                baseline.p50, candidate.p50, candidate.p50 - baseline.p50);
        System.out.printf( "│ p90 latency (ms) │ %7.1f         │ %7.1f         │ %+.1f    │%n",
                baseline.p90, candidate.p90, candidate.p90 - baseline.p90);
        System.out.printf( "│ p99 latency (ms) │ %7.1f         │ %7.1f         │ %+.1f    │%n",
                baseline.p99, candidate.p99, candidate.p99 - baseline.p99);
        System.out.printf( "│ QPS              │ %7.0f         │ %7.0f         │ %+.0f    │%n",
                baseline.qps, candidate.qps, candidate.qps - baseline.qps);
        System.out.printf( "│ build time (ms)  │ %7d         │ %7d         │ %+d    │%n",
                baseline.buildMs, candidate.buildMs, candidate.buildMs - baseline.buildMs);
        System.out.println("└──────────────────┴─────────────────┴─────────────────┴──────────┘");

        double recallDrop = baseline.recall10 - candidate.recall10;
        double buildSpeedup = (baseline.buildMs > 0)
                ? (double) (baseline.buildMs - candidate.buildMs) / baseline.buildMs * 100
                : 0;

        System.out.println();
        System.out.printf("  recall@10 drop: %.4f (%.2f%%)%n", recallDrop, recallDrop / Math.max(0.0001, baseline.recall10) * 100);
        System.out.printf("  build speedup: %.1f%%%n", buildSpeedup);

        if (recallDrop < 0.01 && buildSpeedup > 5) {
            System.out.println("  DECISION: R=32/Lbuild=75 — recall drop <1%, build noticeably faster");
        } else if (recallDrop >= 0.01) {
            System.out.println("  DECISION: R=48/Lbuild=128 — recall drop ≥1%, keep baseline");
        } else {
            System.out.println("  DECISION: R=48/Lbuild=128 — build speedup <5%, not worth the change");
        }
    }

    private Result runConfig(String label, int R, int Lbuild) {
        System.out.printf("%n--- %s: R=%d, Lbuild=%d ---%n", label, R, Lbuild);

        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.delta.maxSize", String.valueOf(N + 1));
        System.setProperty("resonance.index.l2.R", String.valueOf(R));
        System.setProperty("resonance.index.l2.Lbuild", String.valueOf(Lbuild));

        Path dir = tempDir.resolve("graph-" + label);
        WavePatternStoreImpl store = new WavePatternStoreImpl(dir, DIM, runtime);

        try {
            long insertStart = System.nanoTime();
            Random dataRng = new Random(DATA_SEED);
            String[] insertedIds = new String[N];
            for (int i = 0; i < N; i++) {
                try {
                    insertedIds[i] = store.insert(randomPattern(dataRng, DIM), Map.of());
                } catch (Exception e) { /* skip dups */ }
            }
            System.out.printf("  insert: %d ms%n", (System.nanoTime() - insertStart) / 1_000_000);

            long buildStart = System.nanoTime();
            store.forceIndexRebuild();
            long buildMs = (System.nanoTime() - buildStart) / 1_000_000;
            System.out.printf("  build: %d ms%n", buildMs);

            for (int i = 0; i < WARMUP && i < NUM_QUERIES; i++) {
                store.query(queries[i], TOP_K);
            }

            double totalRecall1 = 0, totalRecall10 = 0;
            long[] latencies = new long[NUM_QUERIES];

            for (int q = 0; q < NUM_QUERIES; q++) {
                long t0 = System.nanoTime();
                List<ResonanceMatch> results = store.query(queries[q], TOP_K);
                latencies[q] = System.nanoTime() - t0;

                Set<String> ids = new HashSet<>();
                String firstId = null;
                for (ResonanceMatch r : results) {
                    ids.add(r.id());
                    if (firstId == null) firstId = r.id();
                }

                if (groundTruthIds[q].length > 0 && firstId != null
                        && firstId.equals(groundTruthIds[q][0])) {
                    totalRecall1 += 1.0;
                }

                int hits = 0;
                int gtK = Math.min(TOP_K, groundTruthIds[q].length);
                for (int j = 0; j < gtK; j++) {
                    if (groundTruthIds[q][j] != null && ids.contains(groundTruthIds[q][j])) hits++;
                }
                totalRecall10 += (double) hits / Math.max(1, gtK);
            }

            Arrays.sort(latencies);
            double p50 = latencies[(int)(NUM_QUERIES * 0.50)] / 1_000_000.0;
            double p90 = latencies[(int)(NUM_QUERIES * 0.90)] / 1_000_000.0;
            double p99 = latencies[(int)(NUM_QUERIES * 0.99)] / 1_000_000.0;
            double totalMs = Arrays.stream(latencies).sum() / 1_000_000.0;

            return new Result(
                    totalRecall1 / NUM_QUERIES,
                    totalRecall10 / NUM_QUERIES,
                    p50, p90, p99,
                    NUM_QUERIES / (totalMs / 1000.0),
                    buildMs);

        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
            System.clearProperty("resonance.index.l2.R");
            System.clearProperty("resonance.index.l2.Lbuild");
            try { store.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    private void computeGroundTruth() {
        System.out.println("  computing ground truth...");

        Random dataRng = new Random(DATA_SEED);
        WavePattern[] data = new WavePattern[N];
        String[] ids = new String[N];
        for (int i = 0; i < N; i++) {
            data[i] = randomPattern(dataRng, DIM);
            ids[i] = ai.evacortex.resonancedb.core.storage.util.HashingUtil
                    .computeContentHash(data[i]);
        }

        groundTruthIds = new String[NUM_QUERIES][];
        long t0 = System.nanoTime();
        for (int q = 0; q < NUM_QUERIES; q++) {
            float[] scores = new float[N];
            for (int i = 0; i < N; i++) {
                scores[i] = kernel.compare(queries[q], data[i]);
            }
            int k = Math.min(TOP_K, N);
            int[] idx = new int[N];
            for (int i = 0; i < N; i++) idx[i] = i;
            for (int i = 0; i < k; i++) {
                int best = i;
                for (int j = i + 1; j < N; j++) {
                    if (scores[idx[j]] > scores[idx[best]]) best = j;
                }
                int tmp = idx[i]; idx[i] = idx[best]; idx[best] = tmp;
            }
            groundTruthIds[q] = new String[k];
            for (int i = 0; i < k; i++) groundTruthIds[q][i] = ids[idx[i]];
        }
        System.out.printf("  ground truth: %d ms%n", (System.nanoTime() - t0) / 1_000_000);
    }

    private record Result(double recall1, double recall10,
                          double p50, double p90, double p99,
                          double qps, long buildMs) {}

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }
}
