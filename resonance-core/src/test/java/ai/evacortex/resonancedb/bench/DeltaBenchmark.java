/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.bench;

import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Benchmark for WAL + DeltaBuffer write path (Stage 5).
 *
 * <p>Measures:</p>
 * <ul>
 *   <li>Durable insert throughput (group mode)</li>
 *   <li>Async insert throughput</li>
 *   <li>Query latency with filled delta (5K entries)</li>
 *   <li>Legacy insert throughput (baseline)</li>
 * </ul>
 *
 * <p>Run: {@code ./gradlew :resonance-core:test --tests "*.DeltaBenchmark"}</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("benchmark")
class DeltaBenchmark {

    private static final long SEED = 54321L;
    private static final int DIM = Integer.getInteger("resonance.pattern.len", 1536);
    private static final int TOP_K = 10;

    private static final int BENCH_N = Integer.getInteger("bench.N", 5000);
    private static final int MEASURE_QUERIES = Integer.getInteger("bench.measure", 100);
    private static final int WARMUP_QUERIES = Integer.getInteger("bench.warmup", 20);

    @TempDir
    Path tempDir;

    private StoreRuntimeServices runtime;

    @BeforeAll
    void initRuntime() {
        runtime = StoreRuntimeServices.fromSystemProperties();
    }

    @AfterEach
    void cleanupProperties() {
        clearDeltaProperties();
    }

    @AfterAll
    void closeRuntime() {
        clearDeltaProperties();
        if (runtime != null) runtime.close();
    }

    @Test
    @Order(1)
    @DisplayName("Delta: insert throughput (group commit)")
    void insertThroughputGroup() {
        System.out.println();
        System.out.println("=".repeat(80));
        System.out.println("DeltaBenchmark — WAL group commit insert throughput");
        System.out.println("  dim=" + DIM + ", N=" + BENCH_N);
        System.out.println("  heap: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)) + " MB");
        System.out.println("=".repeat(80));

        System.setProperty("resonance.wal.enabled", "true");
        System.setProperty("resonance.wal.durability", "group");
        System.setProperty("resonance.delta.sealThreshold", String.valueOf(BENCH_N + 1000));
        System.setProperty("resonance.delta.maxEntries", String.valueOf(BENCH_N + 2000));
        System.setProperty("resonance.index.enabled", "false");

        Path storeDir = tempDir.resolve("delta-group");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            Random rng = new Random(SEED);
            long insertStart = System.nanoTime();
            for (int i = 0; i < BENCH_N; i++) {
                store.insert(randomPattern(rng, DIM), Map.of());
            }
            long insertElapsed = System.nanoTime() - insertStart;
            double insertMs = insertElapsed / 1_000_000.0;
            double insertPerSec = BENCH_N / (insertMs / 1000.0);

            System.out.printf("  WAL group insert: %d patterns in %.0f ms (%.0f inserts/sec)%n",
                    BENCH_N, insertMs, insertPerSec);
        } finally {
            store.close();
            clearDeltaProperties();
        }
    }

    @Test
    @Order(2)
    @DisplayName("Delta: insert throughput (async)")
    void insertThroughputAsync() {
        System.out.println();
        System.out.println("--- WAL async insert throughput ---");

        System.setProperty("resonance.wal.enabled", "true");
        System.setProperty("resonance.wal.durability", "async");
        System.setProperty("resonance.delta.sealThreshold", String.valueOf(BENCH_N + 1000));
        System.setProperty("resonance.delta.maxEntries", String.valueOf(BENCH_N + 2000));
        System.setProperty("resonance.index.enabled", "false");

        Path storeDir = tempDir.resolve("delta-async");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            Random rng = new Random(SEED + 1);
            long insertStart = System.nanoTime();
            for (int i = 0; i < BENCH_N; i++) {
                store.insert(randomPattern(rng, DIM), Map.of());
            }
            long insertElapsed = System.nanoTime() - insertStart;
            double insertMs = insertElapsed / 1_000_000.0;
            double insertPerSec = BENCH_N / (insertMs / 1000.0);

            System.out.printf("  WAL async insert: %d patterns in %.0f ms (%.0f inserts/sec)%n",
                    BENCH_N, insertMs, insertPerSec);
        } finally {
            store.close();
            clearDeltaProperties();
        }
    }

    @Test
    @Order(3)
    @DisplayName("Delta: legacy insert throughput (baseline)")
    void insertThroughputLegacy() {
        System.out.println();
        System.out.println("--- Legacy insert throughput (no WAL) ---");

        System.setProperty("resonance.index.enabled", "false");

        Path storeDir = tempDir.resolve("legacy");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            Random rng = new Random(SEED + 2);
            long insertStart = System.nanoTime();
            for (int i = 0; i < BENCH_N; i++) {
                store.insert(randomPattern(rng, DIM), Map.of());
            }
            long insertElapsed = System.nanoTime() - insertStart;
            double insertMs = insertElapsed / 1_000_000.0;
            double insertPerSec = BENCH_N / (insertMs / 1000.0);

            System.out.printf("  Legacy insert: %d patterns in %.0f ms (%.0f inserts/sec)%n",
                    BENCH_N, insertMs, insertPerSec);
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Delta: query latency with filled delta buffer")
    void queryLatencyWithDelta() {
        System.out.println();
        System.out.println("--- Query latency with delta buffer (N=" + BENCH_N + " in delta) ---");

        System.setProperty("resonance.wal.enabled", "true");
        System.setProperty("resonance.wal.durability", "async");
        System.setProperty("resonance.delta.sealThreshold", String.valueOf(BENCH_N + 1000));
        System.setProperty("resonance.delta.maxEntries", String.valueOf(BENCH_N + 2000));
        System.setProperty("resonance.index.enabled", "false");

        Path storeDir = tempDir.resolve("delta-query");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            Random rng = new Random(SEED + 3);
            for (int i = 0; i < BENCH_N; i++) {
                store.insert(randomPattern(rng, DIM), Map.of());
            }

            Random queryRng = new Random(SEED + 9999);
            WavePattern[] queries = new WavePattern[WARMUP_QUERIES + MEASURE_QUERIES];
            for (int i = 0; i < queries.length; i++) {
                queries[i] = randomPattern(queryRng, DIM);
            }

            for (int i = 0; i < WARMUP_QUERIES; i++) {
                store.query(queries[i], TOP_K);
            }

            long[] latenciesNs = new long[MEASURE_QUERIES];
            for (int i = 0; i < MEASURE_QUERIES; i++) {
                long t0 = System.nanoTime();
                List<ResonanceMatch> results = store.query(queries[WARMUP_QUERIES + i], TOP_K);
                latenciesNs[i] = System.nanoTime() - t0;
                assertFalse(results.isEmpty(), "Query should return results from delta");
            }

            Arrays.sort(latenciesNs);
            double p50 = latenciesNs[(int) (MEASURE_QUERIES * 0.50)] / 1_000_000.0;
            double p90 = latenciesNs[(int) (MEASURE_QUERIES * 0.90)] / 1_000_000.0;
            double p99 = latenciesNs[(int) (MEASURE_QUERIES * 0.99)] / 1_000_000.0;
            double mean = Arrays.stream(latenciesNs).average().orElse(0) / 1_000_000.0;

            System.out.printf("  query delta (topK=%d, deltaSize=%d): p50=%.1f ms, p90=%.1f ms, p99=%.1f ms, mean=%.1f ms%n",
                    TOP_K, BENCH_N, p50, p90, p99, mean);

            long sealStart = System.nanoTime();
            store.sealDelta();
            long sealElapsed = System.nanoTime() - sealStart;
            System.out.printf("  seal: %.0f ms%n", sealElapsed / 1_000_000.0);

            for (int i = 0; i < WARMUP_QUERIES; i++) {
                store.query(queries[i], TOP_K);
            }
            long[] postSealLatencies = new long[MEASURE_QUERIES];
            for (int i = 0; i < MEASURE_QUERIES; i++) {
                long t0 = System.nanoTime();
                store.query(queries[WARMUP_QUERIES + i], TOP_K);
                postSealLatencies[i] = System.nanoTime() - t0;
            }

            Arrays.sort(postSealLatencies);
            double postP50 = postSealLatencies[(int) (MEASURE_QUERIES * 0.50)] / 1_000_000.0;
            double postP99 = postSealLatencies[(int) (MEASURE_QUERIES * 0.99)] / 1_000_000.0;

            System.out.printf("  query post-seal: p50=%.1f ms, p99=%.1f ms%n", postP50, postP99);

        } finally {
            store.close();
            clearDeltaProperties();
        }

        System.out.println("=".repeat(80));
    }

    @Test
    @Order(5)
    @DisplayName("Delta: full pipeline with IVF (insert → seal → index → query)")
    void fullPipelineWithIvf() {
        int n = Integer.getInteger("bench.N", 2000);
        System.out.println();
        System.out.println("--- Full pipeline: WAL + DeltaBuffer + IVF (N=" + n + ") ---");

        System.setProperty("resonance.wal.enabled", "true");
        System.setProperty("resonance.wal.durability", "group");
        System.setProperty("resonance.delta.sealThreshold", String.valueOf(n + 100));
        System.setProperty("resonance.delta.maxEntries", String.valueOf(n + 500));
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");

        Path storeDir = tempDir.resolve("delta-ivf");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            Random rng = new Random(SEED + 10);
            long insertStart = System.nanoTime();
            for (int i = 0; i < n; i++) {
                store.insert(randomPattern(rng, DIM), Map.of());
            }
            long insertElapsed = System.nanoTime() - insertStart;
            System.out.printf("  insert %d: %.0f ms (%.0f/sec)%n",
                    n, insertElapsed / 1e6, n / (insertElapsed / 1e9));

            Random queryRng = new Random(SEED + 8888);
            WavePattern[] queries = new WavePattern[WARMUP_QUERIES + MEASURE_QUERIES];
            for (int i = 0; i < queries.length; i++) {
                queries[i] = randomPattern(queryRng, DIM);
            }

            for (int i = 0; i < WARMUP_QUERIES; i++) store.query(queries[i], TOP_K);
            long[] deltaLatencies = new long[MEASURE_QUERIES];
            for (int i = 0; i < MEASURE_QUERIES; i++) {
                long t0 = System.nanoTime();
                store.query(queries[WARMUP_QUERIES + i], TOP_K);
                deltaLatencies[i] = System.nanoTime() - t0;
            }
            Arrays.sort(deltaLatencies);
            System.out.printf("  query (delta, pre-seal): p50=%.1f ms, p99=%.1f ms%n",
                    deltaLatencies[50] / 1e6, deltaLatencies[99] / 1e6);

            long sealStart = System.nanoTime();
            store.sealDelta();
            long sealElapsed = System.nanoTime() - sealStart;
            System.out.printf("  seal: %.0f ms%n", sealElapsed / 1e6);

            long indexStart = System.nanoTime();
            store.forceIndexRebuild();
            long indexElapsed = System.nanoTime() - indexStart;
            System.out.printf("  index rebuild: %.0f ms%n", indexElapsed / 1e6);

            for (int i = 0; i < WARMUP_QUERIES; i++) store.query(queries[i], TOP_K);
            long[] ivfLatencies = new long[MEASURE_QUERIES];
            for (int i = 0; i < MEASURE_QUERIES; i++) {
                long t0 = System.nanoTime();
                store.query(queries[WARMUP_QUERIES + i], TOP_K);
                ivfLatencies[i] = System.nanoTime() - t0;
            }
            Arrays.sort(ivfLatencies);
            double p50 = ivfLatencies[50] / 1e6;
            double p99 = ivfLatencies[99] / 1e6;
            double totalMs = Arrays.stream(ivfLatencies).sum() / 1e6;
            double qps = MEASURE_QUERIES / (totalMs / 1000.0);
            System.out.printf("  query (IVF, post-rebuild): p50=%.1f ms, p99=%.1f ms, QPS=%.0f%n",
                    p50, p99, qps);
            System.out.println("=".repeat(80));

        } finally {
            store.close();
            clearDeltaProperties();
            System.clearProperty("resonance.index.l2.enabled");
        }
    }

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = 0.1 + rng.nextDouble() * 0.9;
            phase[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static void clearDeltaProperties() {
        System.clearProperty("resonance.wal.enabled");
        System.clearProperty("resonance.wal.durability");
        System.clearProperty("resonance.delta.sealThreshold");
        System.clearProperty("resonance.delta.maxEntries");
        System.clearProperty("resonance.index.enabled");
    }
}
