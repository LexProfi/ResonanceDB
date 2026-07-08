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
 * Recall benchmark for query() path only.
 *
 * <p>queryDetailed is excluded: its collectDetailedFromCandidates path performs
 * near-exhaustive scan, giving trivially high recall (~1.0) that does not
 * reflect index quality. queryDetailed recall becomes meaningful only after
 * the two-phase rerank path is implemented (Этап 3).</p>
 *
 * <p>Ground truth: computed via kernel.compare() brute-force in-process against
 * all N patterns (no store query path involved, no phase-sharding artifacts).
 * For N ≤ 20K, patterns are held in memory. For larger N, ground truth is
 * computed via store without IVF in single-bucket mode.</p>
 *
 * <p>Query set: hold-out (separate seed), NOT from indexed data.
 * Self-queries inflate recall and are avoided.</p>
 *
 * <p>Run: {@code ./gradlew :resonance-core:test --tests "*.RecallBenchmark" -Ppreset=heavy -Dbench.N=50000}</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("benchmark")
@Timeout(value = 60, unit = TimeUnit.MINUTES)
class RecallBenchmark {

    private static final long DATA_SEED = 271828L;
    private static final long QUERY_SEED = 161803L;

    private static final int DIM = Integer.getInteger("resonance.pattern.len", 1536);
    // Default N=5K fits in 5 min with default heap. For N=50K use -Ppreset=heavy -Dbench.N=50000
    private static final int N = Integer.getInteger("bench.N", 5_000);
    private static final int NUM_QUERIES = Integer.getInteger("bench.queries", 200);
    private static final int TOP_K = 10;
    private static final int WARMUP_QUERIES = 20;

    @TempDir
    Path tempDir;

    private StoreRuntimeServices runtime;

    @BeforeAll
    void initRuntime() {
        runtime = StoreRuntimeServices.fromSystemProperties();
    }

    @AfterAll
    void closeRuntime() {
        if (runtime != null) runtime.close();
    }

    @Test
    @DisplayName("Recall benchmark: query() via IVF+Vamana")
    void recallBenchmark() {
        int nProbe = Integer.getInteger("resonance.index.l1.nprobe", 8);

        System.out.println();
        System.out.println("=".repeat(80));
        System.out.println("ResonanceDB Recall Benchmark");
        System.out.printf("  N=%d, dim=%d, queries=%d, topK=%d, nProbe=%d%n",
                N, DIM, NUM_QUERIES, TOP_K, nProbe);
        System.out.println("  JVM: " + System.getProperty("java.vm.name") +
                " " + System.getProperty("java.vm.version"));
        System.out.printf("  heap: %d MB, CPUs: %d%n",
                Runtime.getRuntime().maxMemory() / (1024 * 1024),
                Runtime.getRuntime().availableProcessors());
        System.out.println("=".repeat(80));

        // ── Hold-out queries (separate seed, NOT from indexed data) ──────
        WavePattern[] queries = new WavePattern[NUM_QUERIES];
        Random queryRng = new Random(QUERY_SEED);
        for (int i = 0; i < NUM_QUERIES; i++) {
            queries[i] = randomPattern(queryRng, DIM);
        }
        System.out.printf("  queries generated: %d hold-out patterns%n", NUM_QUERIES);

        // ── Insert data into IVF-enabled store ──────────────────────────
        System.setProperty("resonance.index.enabled", "true");
        // Set delta threshold above N to prevent background rebuild during insert
        System.setProperty("resonance.index.delta.maxSize", String.valueOf(N + 1));

        Path storeDir = tempDir.resolve("recall-ivf");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            long insertStart = System.nanoTime();
            Random dataRng = new Random(DATA_SEED);
            WavePattern[] dataPatterns = new WavePattern[N];
            String[] insertedIds = new String[N];
            for (int i = 0; i < N; i++) {
                dataPatterns[i] = randomPattern(dataRng, DIM);
                try {
                    insertedIds[i] = store.insert(dataPatterns[i], Map.of());
                } catch (Exception e) {
                    // skip duplicates
                }
            }
            long insertMs = (System.nanoTime() - insertStart) / 1_000_000;
            System.out.printf("  insert: %d patterns in %d ms (%.0f inserts/sec)%n",
                    N, insertMs, N / (insertMs / 1000.0));

            // ── Ground truth: brute-force kernel.compare in-process ─────
            //    Direct kernel scoring against all N patterns.
            //    No store query path involved → no phase-sharding artifacts.
            ResonanceKernel kernel = new JavaKernel();
            String[][] groundTruthIds = new String[NUM_QUERIES][];

            long gtStart = System.nanoTime();
            for (int q = 0; q < NUM_QUERIES; q++) {
                // Score against all patterns
                float[] scores = new float[N];
                for (int i = 0; i < N; i++) {
                    if (dataPatterns[i] != null) {
                        scores[i] = kernel.compare(queries[q], dataPatterns[i]);
                    }
                }

                // Partial sort: find top-K indices
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
                for (int i = 0; i < k; i++) {
                    groundTruthIds[q][i] = insertedIds[idx[i]];
                }

                if ((q + 1) % 100 == 0) {
                    System.out.printf("  ground truth: %d/%d queries...%n", q + 1, NUM_QUERIES);
                }
            }
            long gtMs = (System.nanoTime() - gtStart) / 1_000_000;
            System.out.printf("  ground truth: %d queries × %d patterns in %d ms%n",
                    NUM_QUERIES, N, gtMs);

            // Free data patterns — no longer needed
            dataPatterns = null;

            // ── Explicit blocking index build ───────────────────────────
            long buildStart = System.nanoTime();
            store.forceIndexRebuild();
            long buildMs = (System.nanoTime() - buildStart) / 1_000_000;
            System.out.printf("  index build: %d ms%n", buildMs);

            // ── Warmup ──────────────────────────────────────────────────
            for (int i = 0; i < WARMUP_QUERIES && i < NUM_QUERIES; i++) {
                store.query(queries[i], TOP_K);
            }

            // ── Measure: recall + latency ───────────────────────────────
            double totalRecall1 = 0;
            double totalRecall10 = 0;
            long[] latenciesNs = new long[NUM_QUERIES];

            for (int q = 0; q < NUM_QUERIES; q++) {
                long t0 = System.nanoTime();
                List<ResonanceMatch> results = store.query(queries[q], TOP_K);
                latenciesNs[q] = System.nanoTime() - t0;

                Set<String> resultIds = new HashSet<>();
                String firstId = null;
                for (ResonanceMatch r : results) {
                    resultIds.add(r.id());
                    if (firstId == null) firstId = r.id();
                }

                // recall@1
                if (groundTruthIds[q].length > 0 && firstId != null
                        && firstId.equals(groundTruthIds[q][0])) {
                    totalRecall1 += 1.0;
                }

                // recall@10
                int hits = 0;
                int gtK = Math.min(TOP_K, groundTruthIds[q].length);
                for (int j = 0; j < gtK; j++) {
                    if (groundTruthIds[q][j] != null && resultIds.contains(groundTruthIds[q][j])) {
                        hits++;
                    }
                }
                totalRecall10 += (double) hits / Math.max(1, gtK);
            }

            double recall1 = totalRecall1 / NUM_QUERIES;
            double recall10 = totalRecall10 / NUM_QUERIES;

            // ── Latency stats ───────────────────────────────────────────
            Arrays.sort(latenciesNs);
            double p50 = latenciesNs[(int) (NUM_QUERIES * 0.50)] / 1_000_000.0;
            double p90 = latenciesNs[(int) (NUM_QUERIES * 0.90)] / 1_000_000.0;
            double p99 = latenciesNs[(int) (NUM_QUERIES * 0.99)] / 1_000_000.0;
            double totalQueryMs = Arrays.stream(latenciesNs).sum() / 1_000_000.0;
            double qps = NUM_QUERIES / (totalQueryMs / 1000.0);

            // ── Results table ───────────────────────────────────────────
            System.out.println();
            System.out.println("┌──────────────┬───────────────┐");
            System.out.println("│ Metric       │ Value         │");
            System.out.println("├──────────────┼───────────────┤");
            System.out.printf( "│ recall@1     │ %.4f        │%n", recall1);
            System.out.printf( "│ recall@10    │ %.4f        │%n", recall10);
            System.out.printf( "│ p50 latency  │ %7.1f ms    │%n", p50);
            System.out.printf( "│ p90 latency  │ %7.1f ms    │%n", p90);
            System.out.printf( "│ p99 latency  │ %7.1f ms    │%n", p99);
            System.out.printf( "│ QPS          │ %7.0f       │%n", qps);
            System.out.printf( "│ build time   │ %7d ms    │%n", buildMs);
            System.out.printf( "│ N            │ %7d       │%n", N);
            System.out.printf( "│ nProbe       │ %7d       │%n", nProbe);
            System.out.printf( "│ queries      │ %7d       │%n", NUM_QUERIES);
            System.out.println("└──────────────┴───────────────┘");

        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
            try { store.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

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
