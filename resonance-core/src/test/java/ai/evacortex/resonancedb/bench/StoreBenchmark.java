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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance benchmark for WavePatternStoreImpl.
 *
 * <p>Measures insert throughput, query latency (p50/p99), and QPS
 * at various dataset sizes. Results printed to stdout for capture.</p>
 *
 * <p>Run with: {@code ./gradlew :resonance-core:test --tests "*.StoreBenchmark" -Ppreset=heavy}</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("benchmark")
class StoreBenchmark {

    private static final long SEED = 12345L;
    private static final int DIM = Integer.getInteger("resonance.pattern.len", 1536);
    private static final int TOP_K = 10;
    private static final int WARMUP_QUERIES = Integer.getInteger("bench.warmup", 20);
    private static final int MEASURE_QUERIES = Integer.getInteger("bench.measure", 100);

    /** Dataset sizes to benchmark. Override with -Dbench.sizes=10000,50000 */
    private static final int[] SIZES = parseSizes(
            System.getProperty("bench.sizes", autoSizes()));

    @TempDir
    Path tempDir;

    private StoreRuntimeServices runtime;

    @BeforeAll
    void initRuntime() {
        runtime = StoreRuntimeServices.fromSystemProperties();
    }

    @AfterAll
    void closeRuntime() {
        if (runtime != null) {
            runtime.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Benchmark: insert + query at various N")
    void benchmarkInsertAndQuery() {
        System.out.println();
        System.out.println("=".repeat(80));
        System.out.println("ResonanceDB Store Benchmark");
        System.out.println("  dim=" + DIM + ", topK=" + TOP_K +
                ", warmup=" + WARMUP_QUERIES + ", measure=" + MEASURE_QUERIES);
        System.out.println("  JVM: " + System.getProperty("java.vm.name") +
                " " + System.getProperty("java.vm.version"));
        System.out.println("  heap: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)) + " MB");
        System.out.println("  CPUs: " + Runtime.getRuntime().availableProcessors());
        System.out.println("=".repeat(80));

        for (int n : SIZES) {
            runBenchmarkForSize(n);
        }

        System.out.println("=".repeat(80));
    }

    private void runBenchmarkForSize(int n) {
        System.out.println();
        System.out.println("--- N=" + n + " ---");

        Path storeDir = tempDir.resolve("bench-" + n);
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);
        Random dataRng = new Random(SEED);

        try {
            // ── Insert phase ─────────────────────────────────────────────
            long insertStart = System.nanoTime();
            for (int i = 0; i < n; i++) {
                WavePattern p = randomPattern(dataRng, DIM);
                try {
                    store.insert(p, Map.of("idx", String.valueOf(i)));
                } catch (Exception e) {
                    // Skip duplicates (extremely unlikely with random data)
                }
            }
            long insertElapsed = System.nanoTime() - insertStart;
            double insertMs = insertElapsed / 1_000_000.0;
            double insertPerSec = n / (insertMs / 1000.0);

            System.out.printf("  insert: %d patterns in %.0f ms (%.0f inserts/sec)%n",
                    n, insertMs, insertPerSec);

            // Force IVF index build after bulk insert (if enabled)
            long indexStart = System.nanoTime();
            store.forceIndexRebuild();
            long indexElapsed = System.nanoTime() - indexStart;
            if (indexElapsed > 1_000_000) { // only report if >1ms (means index was built)
                System.out.printf("  index build: %.0f ms%n", indexElapsed / 1_000_000.0);
            }

            // ── Generate query set ───────────────────────────────────────
            Random queryRng = new Random(SEED + 999);
            WavePattern[] queries = new WavePattern[WARMUP_QUERIES + MEASURE_QUERIES];
            for (int i = 0; i < queries.length; i++) {
                queries[i] = randomPattern(queryRng, DIM);
            }

            // ── Warmup ───────────────────────────────────────────────────
            for (int i = 0; i < WARMUP_QUERIES; i++) {
                store.query(queries[i], TOP_K);
            }

            // ── Measure query latency ────────────────────────────────────
            long[] latenciesNs = new long[MEASURE_QUERIES];
            for (int i = 0; i < MEASURE_QUERIES; i++) {
                long t0 = System.nanoTime();
                List<ResonanceMatch> results = store.query(queries[WARMUP_QUERIES + i], TOP_K);
                latenciesNs[i] = System.nanoTime() - t0;
                assertFalse(results.isEmpty(), "Query should return results");
            }

            Arrays.sort(latenciesNs);
            double p50 = latenciesNs[(int) (MEASURE_QUERIES * 0.50)] / 1_000_000.0;
            double p90 = latenciesNs[(int) (MEASURE_QUERIES * 0.90)] / 1_000_000.0;
            double p99 = latenciesNs[(int) (MEASURE_QUERIES * 0.99)] / 1_000_000.0;
            double mean = Arrays.stream(latenciesNs).average().orElse(0) / 1_000_000.0;
            double totalQueryMs = Arrays.stream(latenciesNs).sum() / 1_000_000.0;
            double qps = MEASURE_QUERIES / (totalQueryMs / 1000.0);

            System.out.printf("  query (topK=%d): p50=%.1f ms, p90=%.1f ms, p99=%.1f ms, mean=%.1f ms%n",
                    TOP_K, p50, p90, p99, mean);
            System.out.printf("  throughput: %.1f QPS%n", qps);

            // ── Measure queryDetailed latency ────────────────────────────
            long[] detLatencies = new long[MEASURE_QUERIES];
            for (int i = 0; i < MEASURE_QUERIES; i++) {
                long t0 = System.nanoTime();
                store.queryDetailed(queries[WARMUP_QUERIES + i], TOP_K);
                detLatencies[i] = System.nanoTime() - t0;
            }

            Arrays.sort(detLatencies);
            double detP50 = detLatencies[(int) (MEASURE_QUERIES * 0.50)] / 1_000_000.0;
            double detP99 = detLatencies[(int) (MEASURE_QUERIES * 0.99)] / 1_000_000.0;

            System.out.printf("  queryDetailed: p50=%.1f ms, p99=%.1f ms%n", detP50, detP99);

        } finally {
            try {
                store.close();
            } catch (Exception e) {
                System.err.println("  [WARN] store.close() failed: " + e.getMessage());
            }
        }
    }

    @Test
    @Order(2)
    @DisplayName("Benchmark: concurrent query throughput")
    void benchmarkConcurrentQueries() {
        int n = SIZES[0]; // use smallest size for concurrency test
        System.out.println();
        System.out.println("--- Concurrent query benchmark (N=" + n + ") ---");

        Path storeDir = tempDir.resolve("bench-concurrent");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);
        Random dataRng = new Random(SEED);

        try {
            // Insert data
            for (int i = 0; i < n; i++) {
                try {
                    store.insert(randomPattern(dataRng, DIM), Map.of());
                } catch (Exception e) { /* skip dups */ }
            }

            // Prepare queries
            Random queryRng = new Random(SEED + 7777);
            int totalQueries = 500;
            WavePattern[] queries = new WavePattern[totalQueries];
            for (int i = 0; i < totalQueries; i++) {
                queries[i] = randomPattern(queryRng, DIM);
            }

            // Warmup
            for (int i = 0; i < 20; i++) {
                store.query(queries[i], TOP_K);
            }

            // Measure with varying thread counts
            for (int threads : new int[]{1, 2, 4, 8}) {
                ExecutorService exec = Executors.newFixedThreadPool(threads);
                int queriesPerThread = totalQueries / threads;

                long start = System.nanoTime();
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    int offset = t * queriesPerThread;
                    futures.add(exec.submit(() -> {
                        for (int i = 0; i < queriesPerThread; i++) {
                            store.query(queries[offset + i], TOP_K);
                        }
                    }));
                }
                for (Future<?> f : futures) {
                    try { f.get(120, TimeUnit.SECONDS); } catch (Exception e) {
                        fail("Concurrent query failed: " + e.getMessage());
                    }
                }
                long elapsed = System.nanoTime() - start;
                double elapsedMs = elapsed / 1_000_000.0;
                double qps = (threads * queriesPerThread) / (elapsedMs / 1000.0);

                System.out.printf("  threads=%d: %d queries in %.0f ms (%.1f QPS)%n",
                        threads, threads * queriesPerThread, elapsedMs, qps);

                exec.shutdown();
            }
        } finally {
            try {
                store.close();
            } catch (Exception e) {
                System.err.println("  [WARN] store.close() failed: " + e.getMessage());
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("Benchmark: recall@K measurement")
    void benchmarkRecall() {
        int n = SIZES[0];
        int numQueries = Integer.getInteger("bench.recallQueries", 200);
        System.out.println();
        System.out.println("--- Recall benchmark (N=" + n + ", queries=" + numQueries + ", topK=" + TOP_K + ") ---");

        // Hold-out queries generated BEFORE data, never indexed
        Random queryRng = new Random(SEED + 777);
        WavePattern[] queries = new WavePattern[numQueries];
        for (int i = 0; i < numQueries; i++) {
            queries[i] = randomPattern(queryRng, DIM);
        }

        // ── Phase 1: Brute-force ground truth (no IVF) ─────────────────
        String[][] groundTruth = new String[numQueries][];
        {
            Path gtDir = tempDir.resolve("recall-gt");
            WavePatternStoreImpl gtStore = new WavePatternStoreImpl(gtDir, DIM, runtime);
            Random dataRng = new Random(SEED);
            try {
                for (int i = 0; i < n; i++) {
                    try { gtStore.insert(randomPattern(dataRng, DIM), Map.of()); }
                    catch (Exception e) { /* skip */ }
                }
                gtStore.forceIndexRebuild();

                long gtStart = System.nanoTime();
                for (int i = 0; i < numQueries; i++) {
                    var results = gtStore.query(queries[i], TOP_K);
                    groundTruth[i] = results.stream().map(r -> r.id()).toArray(String[]::new);
                }
                System.out.printf("  ground truth: %d queries in %.0f ms (brute-force)%n",
                        numQueries, (System.nanoTime() - gtStart) / 1_000_000.0);
            } finally {
                try { gtStore.close(); } catch (Exception e) { /* ignore */ }
            }
        }

        // ── Phase 2: IVF query() recall ─────────────────────────────────
        {
            Path ivfDir = tempDir.resolve("recall-ivf");
            System.setProperty("resonance.index.enabled", "true");
            WavePatternStoreImpl ivfStore = new WavePatternStoreImpl(ivfDir, DIM, runtime);
            Random dataRng = new Random(SEED);
            try {
                for (int i = 0; i < n; i++) {
                    try { ivfStore.insert(randomPattern(dataRng, DIM), Map.of()); }
                    catch (Exception e) { /* skip */ }
                }
                long buildStart = System.nanoTime();
                ivfStore.forceIndexRebuild();
                System.out.printf("  index build: %.0f ms%n",
                        (System.nanoTime() - buildStart) / 1_000_000.0);

                // Measure recall@K for query()
                double totalRecall = 0;
                long queryStart = System.nanoTime();
                for (int i = 0; i < numQueries; i++) {
                    var results = ivfStore.query(queries[i], TOP_K);
                    Set<String> ivfIds = new java.util.HashSet<>();
                    results.forEach(r -> ivfIds.add(r.id()));

                    int hits = 0;
                    for (String gtId : groundTruth[i]) {
                        if (ivfIds.contains(gtId)) hits++;
                    }
                    totalRecall += (double) hits / Math.max(1, groundTruth[i].length);
                }
                double recall = totalRecall / numQueries;
                double queryMs = (System.nanoTime() - queryStart) / 1_000_000.0;
                System.out.printf("  recall@%d: %.4f (%d queries in %.0f ms)%n",
                        TOP_K, recall, numQueries, queryMs);
            } finally {
                System.clearProperty("resonance.index.enabled");
                try { ivfStore.close(); } catch (Exception e) { /* ignore */ }
            }
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static String autoSizes() {
        long maxHeap = Runtime.getRuntime().maxMemory();
        long heapMb = maxHeap / (1024 * 1024);
        // Each pattern dim=1536: ~25KB (amp+phase+overhead). Safe budget = 60% of heap.
        // 512MB → ~12K patterns safe; 1GB → ~25K; 2GB → ~50K; 4GB → ~100K
        if (heapMb >= 4096) return "10000,50000,100000";
        if (heapMb >= 2048) return "10000,50000";
        if (heapMb >= 1024) return "5000,10000";
        return "1000,5000";
    }

    private static int[] parseSizes(String prop) {
        return Arrays.stream(prop.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .mapToInt(Integer::parseInt)
                .toArray();
    }
}
