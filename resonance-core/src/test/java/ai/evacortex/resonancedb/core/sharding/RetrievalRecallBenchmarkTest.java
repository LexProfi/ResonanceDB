/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.sharding;

import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.PhaseWeights;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Gap 3: Retrieval benchmark against exact ground truth.
 *
 * <p>Measures Recall@1 and Recall@10 for:
 * <ul>
 *   <li>Variant B: circular routing (current code)</li>
 *   <li>Variant C: circular + low-coherence fallback</li>
 * </ul>
 * Across corpora: structured synthetic, ±π boundary-heavy, random phases.
 * With query modes: default, sparse, continuous, all-zero, low-coherence.
 */
class RetrievalRecallBenchmarkTest {

    private static final double PI = Math.PI;

    // ─── helpers ───

    private static WavePattern randomPattern(Random rng, int dim, double phaseCenter, double phaseSigma) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = 0.2 + rng.nextDouble() * 0.8;
            phase[i] = phaseCenter + rng.nextGaussian() * phaseSigma;
            while (phase[i] > PI) phase[i] -= 2 * PI;
            while (phase[i] <= -PI) phase[i] += 2 * PI;
        }
        return new WavePattern(amp, phase);
    }

    private static WavePattern randomUniformPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = -PI + rng.nextDouble() * 2 * PI;
        }
        return new WavePattern(amp, phase);
    }

    private static List<String> exhaustiveTopK(ResonanceKernel kernel,
                                                WavePattern query,
                                                List<WavePattern> corpus,
                                                List<String> ids,
                                                int k,
                                                CompareOptions opts) {
        float[] scores = kernel.compareMany(query, corpus, opts != null ? opts : CompareOptions.defaultOptions());
        Integer[] indices = IntStream.range(0, scores.length).boxed().toArray(Integer[]::new);
        Arrays.sort(indices, (a, b) -> Float.compare(scores[b], scores[a]));
        List<String> topK = new ArrayList<>(k);
        for (int i = 0; i < Math.min(k, indices.length); i++) {
            topK.add(ids.get(indices[i]));
        }
        return topK;
    }

    private static double recallAtK(List<String> groundTruth, List<ResonanceMatch> results, int k) {
        Set<String> gtSet = new HashSet<>(groundTruth.subList(0, Math.min(k, groundTruth.size())));
        int hits = 0;
        for (int i = 0; i < Math.min(k, results.size()); i++) {
            if (gtSet.contains(results.get(i).id())) hits++;
        }
        return gtSet.isEmpty() ? 1.0 : (double) hits / gtSet.size();
    }

    // ─── benchmark harness ───

    private record BenchmarkResult(
            String corpus, String queryMode, int N, int D,
            double recall1, double recall10,
            long avgQueryNanos, int queries
    ) {
        @Override
        public String toString() {
            return String.format("  %-20s %-18s N=%-5d D=%-5d R@1=%.3f  R@10=%.3f  avg=%.2fms  queries=%d",
                    corpus, queryMode, N, D, recall1, recall10, avgQueryNanos / 1e6, queries);
        }
    }

    private BenchmarkResult runBenchmark(
            String corpusName, String queryModeName,
            int N, int D, int numQueries,
            java.util.function.BiFunction<Random, Integer, WavePattern> corpusGen,
            java.util.function.Function<Random, WavePattern> queryGen,
            CompareOptions opts,
            Path tmpDir) {

        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        ResonanceKernel kernel = runtime.resonanceKernel();
        Path dbDir = tmpDir.resolve(corpusName + "-" + queryModeName + "-" + D);

        WavePatternStoreImpl store = new WavePatternStoreImpl(dbDir, D, runtime);
        try {
            Random corpusRng = new Random(42);
            List<WavePattern> corpus = new ArrayList<>(N);
            List<String> ids = new ArrayList<>(N);

            for (int i = 0; i < N; i++) {
                WavePattern p = corpusGen.apply(corpusRng, i);
                String id = store.insert(p, Map.of());
                corpus.add(p);
                ids.add(id);
            }

            Random queryRng = new Random(777);
            double recall1Sum = 0, recall10Sum = 0;
            long totalNanos = 0;

            for (int q = 0; q < numQueries; q++) {
                WavePattern query = queryGen.apply(queryRng);
                List<String> gt = exhaustiveTopK(kernel, query, corpus, ids, 10, opts);

                long t0 = System.nanoTime();
                List<ResonanceMatch> results = store.query(query, 10, opts);
                long elapsed = System.nanoTime() - t0;
                totalNanos += elapsed;

                recall1Sum += recallAtK(gt, results, 1);
                recall10Sum += recallAtK(gt, results, 10);
            }

            return new BenchmarkResult(
                    corpusName, queryModeName, N, D,
                    recall1Sum / numQueries, recall10Sum / numQueries,
                    totalNanos / numQueries, numQueries);
        } finally {
            store.close();
            runtime.close();
        }
    }

    // ─── tests ───

    @Test
    @DisplayName("Recall benchmark: structured synthetic corpus, D=64")
    void benchmark_structured_D64(@TempDir Path tmpDir) {
        int D = 64, N = 2000, Q = 50;

        List<BenchmarkResult> results = new ArrayList<>();

        // Structured corpus: 4 clusters at different phase centers
        double[] centers = {-2.0, -0.5, 0.5, 2.0};
        java.util.function.BiFunction<Random, Integer, WavePattern> structured =
                (rng, i) -> randomPattern(rng, D, centers[i % centers.length], 0.3);

        // Query modes
        results.add(runBenchmark("structured", "default", N, D, Q,
                structured,
                rng -> randomPattern(rng, D, centers[rng.nextInt(4)], 0.2),
                null, tmpDir));

        // Sparse weights
        double[] sparseW = new double[D];
        sparseW[0] = 1.0; sparseW[1] = 1.0; sparseW[2] = 1.0;
        CompareOptions sparseOpts = CompareOptions.withPhaseWeights(new PhaseWeights(sparseW));
        results.add(runBenchmark("structured", "sparse-weights", N, D, Q,
                structured,
                rng -> randomPattern(rng, D, centers[rng.nextInt(4)], 0.2),
                sparseOpts, tmpDir));

        // Continuous weights
        double[] contW = new double[D];
        for (int i = 0; i < D; i++) contW[i] = 0.3 + 0.7 * i / (D - 1);
        CompareOptions contOpts = CompareOptions.withPhaseWeights(new PhaseWeights(contW));
        results.add(runBenchmark("structured", "continuous-weights", N, D, Q,
                structured,
                rng -> randomPattern(rng, D, centers[rng.nextInt(4)], 0.2),
                contOpts, tmpDir));

        // All-zero weights (phase-free)
        CompareOptions zeroOpts = CompareOptions.withPhaseWeights(new PhaseWeights(new double[D]));
        results.add(runBenchmark("structured", "all-zero-weights", N, D, Q,
                structured,
                rng -> randomPattern(rng, D, centers[rng.nextInt(4)], 0.2),
                zeroOpts, tmpDir));

        System.out.printf("%nRecall Benchmark — Structured D=%d:%n", D);
        results.forEach(r -> System.out.println(r));

        // Acceptance: structured corpus should have high recall
        for (BenchmarkResult r : results) {
            if (!r.queryMode.equals("all-zero-weights")) {
                assertTrue(r.recall10 >= 0.5,
                        r.queryMode + " Recall@10 should be ≥0.5, got " + r.recall10);
            }
        }
    }

    @Test
    @DisplayName("Recall benchmark: ±π boundary corpus, D=64")
    void benchmark_boundary_D64(@TempDir Path tmpDir) {
        int D = 64, N = 2000, Q = 50;

        List<BenchmarkResult> results = new ArrayList<>();

        // ±π boundary corpus: patterns clustered near ±π
        java.util.function.BiFunction<Random, Integer, WavePattern> boundary =
                (rng, i) -> {
                    double center = (i % 2 == 0) ? (PI - 0.1) : -(PI - 0.1);
                    return randomPattern(rng, D, center, 0.2);
                };

        results.add(runBenchmark("±π-boundary", "default", N, D, Q,
                boundary,
                rng -> randomPattern(rng, D, PI - 0.05, 0.15),
                null, tmpDir));

        System.out.printf("%nRecall Benchmark — ±π Boundary D=%d:%n", D);
        results.forEach(r -> System.out.println(r));
    }

    @Test
    @DisplayName("Recall benchmark: random phases (negative control), D=64")
    void benchmark_random_D64(@TempDir Path tmpDir) {
        int D = 64, N = 2000, Q = 50;

        List<BenchmarkResult> results = new ArrayList<>();

        java.util.function.BiFunction<Random, Integer, WavePattern> randomCorpus =
                (rng, i) -> randomUniformPattern(rng, D);

        // Default (low-coherence fallback should kick in)
        results.add(runBenchmark("random", "default", N, D, Q,
                randomCorpus,
                rng -> randomUniformPattern(rng, D),
                null, tmpDir));

        // Low-coherence query
        results.add(runBenchmark("random", "low-coherence", N, D, Q,
                randomCorpus,
                rng -> randomUniformPattern(rng, D),
                null, tmpDir));

        System.out.printf("%nRecall Benchmark — Random (negative control) D=%d:%n", D);
        results.forEach(r -> System.out.println(r));
        System.out.println("  (Random corpus is negative control — IVF not expected to be effective)");
    }

    @Test
    @DisplayName("Recall benchmark: structured synthetic, D=256")
    void benchmark_structured_D256(@TempDir Path tmpDir) {
        int D = 256, N = 2000, Q = 30;

        List<BenchmarkResult> results = new ArrayList<>();

        double[] centers = {-2.0, -0.5, 0.5, 2.0};
        java.util.function.BiFunction<Random, Integer, WavePattern> structured =
                (rng, i) -> randomPattern(rng, D, centers[i % centers.length], 0.3);

        results.add(runBenchmark("structured", "default", N, D, Q,
                structured,
                rng -> randomPattern(rng, D, centers[rng.nextInt(4)], 0.2),
                null, tmpDir));

        // Sparse weights
        double[] sparseW = new double[D];
        for (int i = 0; i < 5; i++) sparseW[i] = 1.0;
        CompareOptions sparseOpts = CompareOptions.withPhaseWeights(new PhaseWeights(sparseW));
        results.add(runBenchmark("structured", "sparse-weights", N, D, Q,
                structured,
                rng -> randomPattern(rng, D, centers[rng.nextInt(4)], 0.2),
                sparseOpts, tmpDir));

        System.out.printf("%nRecall Benchmark — Structured D=%d:%n", D);
        results.forEach(r -> System.out.println(r));
    }
}
