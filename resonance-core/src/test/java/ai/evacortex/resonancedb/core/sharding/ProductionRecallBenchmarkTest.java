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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Production-shaped recall benchmark: N=10,000, D=1536, 64 shards.
 *
 * <p>Compares three routing variants on identical fixtures:
 * <ul>
 *   <li>A — legacy arithmetic mean routing (simulated)</li>
 *   <li>B — circular routing, no coherence fallback</li>
 *   <li>C — circular routing + low-coherence fallback (R &lt; 0.15)</li>
 * </ul>
 *
 * <p>Ground truth: exhaustive scoring via ResonanceKernel.compareMany.
 *
 * <p>Also measures store.query() recall to identify ANN pipeline loss
 * separate from routing loss.
 */
class ProductionRecallBenchmarkTest {

    private String savedIndexEnabled;

    @BeforeEach
    void saveProperties() {
        savedIndexEnabled = System.getProperty("resonance.index.enabled");
    }

    @AfterEach
    void restoreProperties() {
        if (savedIndexEnabled == null) {
            System.clearProperty("resonance.index.enabled");
        } else {
            System.setProperty("resonance.index.enabled", savedIndexEnabled);
        }
    }

    private static final double PI = Math.PI;
    private static final double TWO_PI = 2 * PI;

    private static final int BUCKETS = 64;
    private static final double BUCKET_WIDTH = TWO_PI / BUCKETS;
    private static final double READ_EPS = 0.1;
    private static final double LOW_COH_THRESHOLD = 0.15;
    private static final int TOP_K = 10;
    private static final long SEED = 42L;
    private static final long QUERY_SEED = 777L;

    // ─── pattern generators ───

    private static WavePattern structuredPattern(Random rng, int dim, int index) {
        double[] centers = {-2.0, -0.5, 0.5, 2.0};
        double center = centers[index % centers.length];
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = 0.2 + rng.nextDouble() * 0.8;
            double p = center + rng.nextGaussian() * 0.3;
            while (p > PI) p -= TWO_PI;
            while (p <= -PI) p += TWO_PI;
            phase[i] = p;
        }
        return new WavePattern(amp, phase);
    }

    private static WavePattern boundaryPattern(Random rng, int dim, int index) {
        double center = (index % 2 == 0) ? (PI - 0.1) : -(PI - 0.1);
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = 0.2 + rng.nextDouble() * 0.8;
            double p = center + rng.nextGaussian() * 0.2;
            while (p > PI) p -= TWO_PI;
            while (p <= -PI) p += TWO_PI;
            phase[i] = p;
        }
        return new WavePattern(amp, phase);
    }

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = -PI + rng.nextDouble() * TWO_PI;
        }
        return new WavePattern(amp, phase);
    }

    // ─── circular/arithmetic mean, resultant R ───

    private static double circularMean(double[] phase) {
        double sinSum = 0, cosSum = 0;
        for (double p : phase) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        if (sinSum == 0 && cosSum == 0) return 0;
        return Math.atan2(sinSum, cosSum);
    }

    private static double arithmeticMean(double[] phase) {
        double sum = 0;
        for (double p : phase) sum += p;
        return sum / phase.length;
    }

    private static double resultantLength(double[] phase) {
        double sinSum = 0, cosSum = 0;
        for (double p : phase) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        return Math.sqrt(sinSum * sinSum + cosSum * cosSum) / phase.length;
    }

    private static double circularMeanWeighted(double[] phase, double[] weights) {
        double sinSum = 0, cosSum = 0;
        for (int i = 0; i < phase.length; i++) {
            sinSum += weights[i] * Math.sin(phase[i]);
            cosSum += weights[i] * Math.cos(phase[i]);
        }
        if (sinSum == 0 && cosSum == 0) return 0;
        return Math.atan2(sinSum, cosSum);
    }

    private static double arithmeticMeanWeighted(double[] phase, double[] weights) {
        double wSum = 0, pSum = 0;
        for (int i = 0; i < phase.length; i++) {
            wSum += weights[i];
            pSum += weights[i] * phase[i];
        }
        return wSum > 0 ? pSum / wSum : 0;
    }

    // ─── shard routing simulation ───

    private static int phaseBucket(double phaseCenter) {
        double norm = phaseCenter + PI; // shift to [0, 2π)
        if (norm < 0) norm += TWO_PI;
        if (norm >= TWO_PI) norm -= TWO_PI;
        return Math.min((int) (norm / BUCKET_WIDTH), BUCKETS - 1);
    }

    /** Returns set of bucket indices within [queryBucket-range, queryBucket+range]. */
    private static Set<Integer> selectBuckets(double queryPhase, double eps) {
        int center = phaseBucket(queryPhase);
        int range = Math.max(1, (int) Math.ceil(eps / BUCKET_WIDTH));
        Set<Integer> buckets = new LinkedHashSet<>();
        for (int d = -range; d <= range; d++) {
            buckets.add(Math.floorMod(center + d, BUCKETS));
        }
        return buckets;
    }

    // ─── exhaustive ground truth ───

    private static List<String> exhaustiveTopK(ResonanceKernel kernel,
                                                WavePattern query,
                                                List<WavePattern> corpus,
                                                List<String> ids,
                                                int k,
                                                CompareOptions opts) {
        float[] scores = kernel.compareMany(query, corpus,
                opts != null ? opts : CompareOptions.defaultOptions());
        Integer[] indices = IntStream.range(0, scores.length).boxed().toArray(Integer[]::new);
        Arrays.sort(indices, (a, b) -> Float.compare(scores[b], scores[a]));
        List<String> topK = new ArrayList<>(k);
        for (int i = 0; i < Math.min(k, indices.length); i++) {
            topK.add(ids.get(indices[i]));
        }
        return topK;
    }

    // ─── recall computation ───

    private static double recallAtK(List<String> groundTruth, List<String> results, int k) {
        Set<String> gtSet = new HashSet<>(groundTruth.subList(0, Math.min(k, groundTruth.size())));
        int hits = 0;
        for (int i = 0; i < Math.min(k, results.size()); i++) {
            if (gtSet.contains(results.get(i))) hits++;
        }
        return gtSet.isEmpty() ? 1.0 : (double) hits / gtSet.size();
    }

    private static double recallAtKFromMatches(List<String> groundTruth,
                                                List<ResonanceMatch> results, int k) {
        Set<String> gtSet = new HashSet<>(groundTruth.subList(0, Math.min(k, groundTruth.size())));
        int hits = 0;
        for (int i = 0; i < Math.min(k, results.size()); i++) {
            if (gtSet.contains(results.get(i).id())) hits++;
        }
        return gtSet.isEmpty() ? 1.0 : (double) hits / gtSet.size();
    }

    // ─── simulated variant query ───

    /**
     * Simulates shard-routed top-K by selecting candidate patterns from matching buckets,
     * then scoring with the kernel.
     *
     * @return (topK ids, visited bucket count, candidate count)
     */
    private static long[] simulatedQuery(ResonanceKernel kernel,
                                          WavePattern query,
                                          List<WavePattern> corpus,
                                          List<String> ids,
                                          int[] patternBuckets,
                                          Set<Integer> selectedBuckets,
                                          int k,
                                          CompareOptions opts,
                                          List<String> outTopK) {
        List<WavePattern> candidates = new ArrayList<>();
        List<String> candIds = new ArrayList<>();
        for (int i = 0; i < corpus.size(); i++) {
            if (selectedBuckets.contains(patternBuckets[i])) {
                candidates.add(corpus.get(i));
                candIds.add(ids.get(i));
            }
        }

        if (candidates.isEmpty()) {
            return new long[]{selectedBuckets.size(), 0};
        }

        float[] scores = kernel.compareMany(query, candidates,
                opts != null ? opts : CompareOptions.defaultOptions());
        Integer[] indices = IntStream.range(0, scores.length).boxed().toArray(Integer[]::new);
        Arrays.sort(indices, (a, b) -> Float.compare(scores[b], scores[a]));
        for (int i = 0; i < Math.min(k, indices.length); i++) {
            outTopK.add(candIds.get(indices[i]));
        }
        return new long[]{selectedBuckets.size(), candidates.size()};
    }

    // ─── percentile helper ───

    private static long percentile(long[] sortedValues, double p) {
        if (sortedValues.length == 0) return 0;
        int idx = (int) Math.ceil(p / 100.0 * sortedValues.length) - 1;
        return sortedValues[Math.max(0, Math.min(idx, sortedValues.length - 1))];
    }

    // ─── shard occupancy stats ───

    private static double occupancyCV(int[] patternBuckets, int totalPatterns) {
        int[] counts = new int[BUCKETS];
        for (int b : patternBuckets) counts[b]++;
        double mean = (double) totalPatterns / BUCKETS;
        double varSum = 0;
        for (int c : counts) varSum += (c - mean) * (c - mean);
        double std = Math.sqrt(varSum / BUCKETS);
        return mean > 0 ? std / mean : 0;
    }

    // ─── query profile definitions ───

    private enum QueryProfile {
        DEFAULT("default", null),
        SPARSE("sparse-binary", null),       // set in code
        CONTINUOUS("continuous", null),       // set in code
        ALL_ZERO("all-zero", null),           // set in code
        LOW_COHERENCE("low-coherence", null); // query generator override

        final String name;
        final CompareOptions opts;

        QueryProfile(String name, CompareOptions opts) {
            this.name = name;
            this.opts = opts;
        }
    }

    // ─── result record ───

    private record VariantResult(
            String variant, String corpus, String queryProfile,
            int N, int D,
            double recall1, double recall10,
            long p50Nanos, long p95Nanos,
            double avgVisitedShards, double avgCandidates,
            double fallbackRate, double shardOccupancyCV,
            int numQueries
    ) {
        void print() {
            System.out.printf("  %-10s %-16s %-18s N=%-6d D=%-5d R@1=%.3f R@10=%.3f "
                            + "p50=%.2fms p95=%.2fms shards=%.1f cands=%.0f fbRate=%.3f CV=%.3f q=%d%n",
                    variant, corpus, queryProfile, N, D,
                    recall1, recall10,
                    p50Nanos / 1e6, p95Nanos / 1e6,
                    avgVisitedShards, avgCandidates,
                    fallbackRate, shardOccupancyCV, numQueries);
        }
    }

    // ─── main benchmark runner ───

    private List<VariantResult> runFullBenchmark(
            String corpusName,
            int N, int D, int numQueries,
            java.util.function.BiFunction<Random, Integer, WavePattern> corpusGen,
            java.util.function.BiFunction<Random, QueryProfile, WavePattern> queryGen,
            boolean includeStore,
            Path tmpDir) {

        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        ResonanceKernel kernel = runtime.resonanceKernel();

        // ─── 1. Generate corpus ───
        Random corpusRng = new Random(SEED);
        List<WavePattern> corpus = new ArrayList<>(N);
        List<String> ids = new ArrayList<>(N);

        // Assign buckets using circular mean (current production routing)
        int[] circBuckets = new int[N];
        // Assign buckets using arithmetic mean (legacy routing)
        int[] arithBuckets = new int[N];

        for (int i = 0; i < N; i++) {
            WavePattern p = corpusGen.apply(corpusRng, i);
            corpus.add(p);
            ids.add("p-" + i);

            double cMean = circularMean(p.phase());
            double aMean = arithmeticMean(p.phase());
            circBuckets[i] = phaseBucket(cMean);
            arithBuckets[i] = phaseBucket(aMean);
        }

        // Optionally insert into real store for store.query comparison
        WavePatternStoreImpl store = null;
        List<String> storeIds = null;
        if (includeStore) {
            Path storeDir = tmpDir.resolve(corpusName + "-store");
            store = new WavePatternStoreImpl(storeDir, D, runtime);
            storeIds = new ArrayList<>(N);
            try {
                corpusRng = new Random(SEED);
                for (int i = 0; i < N; i++) {
                    WavePattern p = corpusGen.apply(corpusRng, i);
                    String storeId = store.insert(p, Map.of());
                    storeIds.add(storeId);
                }
            } catch (Exception e) {
                System.err.println("WARN: store insertion error: " + e.getMessage());
            }
        }

        double circCV = occupancyCV(circBuckets, N);
        double arithCV = occupancyCV(arithBuckets, N);

        // ─── 2. Build query profiles ───
        double[] sparseW = new double[D];
        for (int i = 0; i < Math.min(5, D); i++) sparseW[i] = 1.0;
        CompareOptions sparseOpts = CompareOptions.withPhaseWeights(new PhaseWeights(sparseW));

        double[] contW = new double[D];
        for (int i = 0; i < D; i++) contW[i] = 0.3 + 0.7 * i / (D - 1);
        CompareOptions contOpts = CompareOptions.withPhaseWeights(new PhaseWeights(contW));

        CompareOptions zeroOpts = CompareOptions.withPhaseWeights(new PhaseWeights(new double[D]));

        List<VariantResult> allResults = new ArrayList<>();

        QueryProfile[] profiles = {QueryProfile.DEFAULT, QueryProfile.SPARSE,
                QueryProfile.CONTINUOUS, QueryProfile.ALL_ZERO, QueryProfile.LOW_COHERENCE};

        for (QueryProfile profile : profiles) {
            CompareOptions opts;
            switch (profile) {
                case SPARSE -> opts = sparseOpts;
                case CONTINUOUS -> opts = contOpts;
                case ALL_ZERO -> opts = zeroOpts;
                default -> opts = null;
            }

            double[] optsWeights = null;
            if (opts != null && opts.phaseWeights() != null) {
                optsWeights = opts.phaseWeights().rawWeights();
            }

            // Per-query accumulators for each variant
            double[] recall1 = new double[4]; // A, B, C, store
            double[] recall10 = new double[4];
            long[][] latencies = new long[4][numQueries];
            double[] visitedShards = new double[4];
            double[] candidates = new double[4];
            int[] fallbackCount = new int[4];

            Random queryRng = new Random(QUERY_SEED);

            for (int q = 0; q < numQueries; q++) {
                WavePattern query;
                if (profile == QueryProfile.LOW_COHERENCE) {
                    query = randomPattern(queryRng, D);
                } else {
                    query = queryGen.apply(queryRng, profile);
                }

                // Ground truth
                List<String> gt = exhaustiveTopK(kernel, query, corpus, ids, TOP_K, opts);

                // Store-ID ground truth (for store.query comparison)
                List<String> gtStore = (store != null)
                        ? exhaustiveTopK(kernel, query, corpus, storeIds, TOP_K, opts)
                        : null;

                double R = resultantLength(query.phase());
                double cQueryPhase = circularMean(query.phase());
                double aQueryPhase = arithmeticMean(query.phase());

                if (optsWeights != null) {
                    cQueryPhase = circularMeanWeighted(query.phase(), optsWeights);
                    aQueryPhase = arithmeticMeanWeighted(query.phase(), optsWeights);
                }

                // ── Variant A: legacy arithmetic routing ──
                long t0 = System.nanoTime();
                Set<Integer> aBuckets = selectBuckets(aQueryPhase, READ_EPS);
                List<String> aTopK = new ArrayList<>();
                long[] aStats = simulatedQuery(kernel, query, corpus, ids, arithBuckets,
                        aBuckets, TOP_K, opts, aTopK);
                latencies[0][q] = System.nanoTime() - t0;
                recall1[0] += recallAtK(gt, aTopK, 1);
                recall10[0] += recallAtK(gt, aTopK, TOP_K);
                visitedShards[0] += aStats[0];
                candidates[0] += aStats[1];

                // ── Variant B: circular routing, no fallback ──
                t0 = System.nanoTime();
                Set<Integer> bBuckets = selectBuckets(cQueryPhase, READ_EPS);
                List<String> bTopK = new ArrayList<>();
                long[] bStats = simulatedQuery(kernel, query, corpus, ids, circBuckets,
                        bBuckets, TOP_K, opts, bTopK);
                latencies[1][q] = System.nanoTime() - t0;
                recall1[1] += recallAtK(gt, bTopK, 1);
                recall10[1] += recallAtK(gt, bTopK, TOP_K);
                visitedShards[1] += bStats[0];
                candidates[1] += bStats[1];

                // ── Variant C: circular routing + coherence fallback ──
                t0 = System.nanoTime();
                Set<Integer> cBuckets;
                boolean fallback = R < LOW_COH_THRESHOLD;
                if (fallback) {
                    cBuckets = new LinkedHashSet<>();
                    for (int b = 0; b < BUCKETS; b++) cBuckets.add(b);
                    fallbackCount[2]++;
                } else {
                    cBuckets = selectBuckets(cQueryPhase, READ_EPS);
                }
                List<String> cTopK = new ArrayList<>();
                long[] cStats = simulatedQuery(kernel, query, corpus, ids, circBuckets,
                        cBuckets, TOP_K, opts, cTopK);
                latencies[2][q] = System.nanoTime() - t0;
                recall1[2] += recallAtK(gt, cTopK, 1);
                recall10[2] += recallAtK(gt, cTopK, TOP_K);
                visitedShards[2] += cStats[0];
                candidates[2] += cStats[1];

                // ── Store.query (actual pipeline, variant C in production) ──
                if (store != null && gtStore != null) {
                    t0 = System.nanoTime();
                    List<ResonanceMatch> storeResults = store.query(query, TOP_K, opts);
                    latencies[3][q] = System.nanoTime() - t0;
                    recall1[3] += recallAtKFromMatches(gtStore, storeResults, 1);
                    recall10[3] += recallAtKFromMatches(gtStore, storeResults, TOP_K);
                    if (fallback) fallbackCount[3]++;
                }
            }

            // Sort latencies for percentiles
            int variantCount = (store != null) ? 4 : 3;
            for (int v = 0; v < variantCount; v++) Arrays.sort(latencies[v]);

            String[] variantNames = {"A-arith", "B-circ", "C-circ+fb", "store"};
            double[] cvs = {arithCV, circCV, circCV, circCV};

            for (int v = 0; v < variantCount; v++) {
                allResults.add(new VariantResult(
                        variantNames[v], corpusName, profile.name,
                        N, D,
                        recall1[v] / numQueries,
                        recall10[v] / numQueries,
                        percentile(latencies[v], 50),
                        percentile(latencies[v], 95),
                        visitedShards[v] / numQueries,
                        candidates[v] / numQueries,
                        (double) fallbackCount[v] / numQueries,
                        cvs[v],
                        numQueries));
            }
        }

        if (store != null) store.close();
        runtime.close();

        return allResults;
    }

    // ─── tests ───

    @Test
    @Tag("benchmark")
    @DisplayName("Production benchmark: N=10000, D=1536, structured corpus")
    void benchmark_structured_10k(@TempDir Path tmpDir) {
        int N = 10_000, D = 1536, Q = 50;

        List<VariantResult> results = runFullBenchmark(
                "structured", N, D, Q,
                (rng, i) -> structuredPattern(rng, D, i),
                (rng, profile) -> {
                    double[] centers = {-2.0, -0.5, 0.5, 2.0};
                    return structuredPattern(rng, D, rng.nextInt(4));
                },
                true, tmpDir);

        System.out.printf("%n═══ Production Benchmark: Structured N=%d D=%d ═══%n", N, D);
        System.out.println("  Variant    Corpus           QueryProfile       N        D     R@1   R@10  "
                + "p50        p95        shards cands    fbRate  CV      q");
        results.forEach(VariantResult::print);

        assertFalse(results.isEmpty(), "benchmark should produce results");
        for (VariantResult r : results) {
            assertTrue(r.recall10 > 0, "structured corpus R@10 should be > 0 for " + r.variant);
        }
    }

    @Test
    @Tag("benchmark")
    @DisplayName("Production benchmark: N=10000, D=1536, ±π boundary corpus")
    void benchmark_boundary_10k(@TempDir Path tmpDir) {
        int N = 10_000, D = 1536, Q = 50;

        List<VariantResult> results = runFullBenchmark(
                "±π-boundary", N, D, Q,
                (rng, i) -> boundaryPattern(rng, D, i),
                (rng, profile) -> boundaryPattern(rng, D, rng.nextInt(2)),
                false, tmpDir);

        System.out.printf("%n═══ Production Benchmark: ±π Boundary N=%d D=%d ═══%n", N, D);
        System.out.println("  Variant    Corpus           QueryProfile       N        D     R@1   R@10  "
                + "p50        p95        shards cands    fbRate  CV      q");
        results.forEach(VariantResult::print);

        assertFalse(results.isEmpty(), "benchmark should produce results");
        for (VariantResult r : results) {
            assertTrue(r.recall10 > 0, "boundary corpus R@10 should be > 0 for " + r.variant);
        }
    }

    @Test
    @Tag("benchmark")
    @DisplayName("Production benchmark: N=10000, D=1536, random (negative control)")
    void benchmark_random_10k(@TempDir Path tmpDir) {
        int N = 10_000, D = 1536, Q = 50;

        List<VariantResult> results = runFullBenchmark(
                "random", N, D, Q,
                (rng, i) -> randomPattern(rng, D),
                (rng, profile) -> randomPattern(rng, D),
                false, tmpDir);

        System.out.printf("%n═══ Production Benchmark: Random (negative control) N=%d D=%d ═══%n", N, D);
        System.out.println("  Variant    Corpus           QueryProfile       N        D     R@1   R@10  "
                + "p50        p95        shards cands    fbRate  CV      q");
        results.forEach(VariantResult::print);

        assertFalse(results.isEmpty(), "benchmark should produce results");
    }
}
