/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.bench;

import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.PhaseRoutingProfile;
import ai.evacortex.resonancedb.core.engine.PhaseWeights;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.index.CentroidIndex;
import ai.evacortex.resonancedb.core.index.IvfCandidateSource;
import ai.evacortex.resonancedb.core.index.ResonanceMoments;
import ai.evacortex.resonancedb.core.math.UnfoldedMath;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive IVF routing diagnostic for Iteration 032:
 * <ul>
 *   <li>P1: GT centroid coverage analysis with percentage-based budgets</li>
 *   <li>P2: Moment score correlation with actual centroid usefulness</li>
 *   <li>P3: Alternative moment scoring formulas comparison</li>
 *   <li>P12: Exhaustive scan latency baseline</li>
 *   <li>P13: Default path regression check</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("benchmark")
@Timeout(value = 60, unit = TimeUnit.MINUTES)
class IvfRoutingDiagnosticTest {

    private static final long DATA_SEED = 271828L;
    private static final long QUERY_SEED = 161803L;
    private static final long MASK_SEED = 577215L;

    private static final int DIM = Integer.getInteger("bench.dim", 1536);
    private static final int N = Integer.getInteger("bench.N", 2000);
    private static final int NUM_QUERIES = Integer.getInteger("bench.queries", 20);
    private static final int TOP_K = 10;

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

    enum MaskType {
        DEFAULT("default (null)"),
        ALL_ONE("all-one"),
        ALL_ZERO("all-zero (phase-free)"),
        SPARSE("sparse (10% active)"),
        DENSE("dense (90% active)"),
        CONTINUOUS("continuous ramp"),
        ADVERSARIAL("adversarial (alternating)");

        final String label;
        MaskType(String label) { this.label = label; }
    }

    private CompareOptions optionsFor(MaskType type, int dim, Random rng) {
        return switch (type) {
            case DEFAULT -> null;
            case ALL_ONE -> CompareOptions.withPhaseWeights(new PhaseWeights(ones(dim)));
            case ALL_ZERO -> CompareOptions.withPhaseWeights(new PhaseWeights(new double[dim]));
            case SPARSE -> CompareOptions.withPhaseWeights(new PhaseWeights(sparseMask(dim, 0.10, rng)));
            case DENSE -> CompareOptions.withPhaseWeights(new PhaseWeights(denseMask(dim, 0.90, rng)));
            case CONTINUOUS -> CompareOptions.withPhaseWeights(new PhaseWeights(continuousRamp(dim)));
            case ADVERSARIAL -> CompareOptions.withPhaseWeights(new PhaseWeights(adversarialMask(dim)));
        };
    }

    private PhaseWeights weightsFor(MaskType type, int dim, Random rng) {
        return switch (type) {
            case DEFAULT, ALL_ONE -> null;
            case ALL_ZERO -> new PhaseWeights(new double[dim]);
            case SPARSE -> new PhaseWeights(sparseMask(dim, 0.10, rng));
            case DENSE -> new PhaseWeights(denseMask(dim, 0.90, rng));
            case CONTINUOUS -> new PhaseWeights(continuousRamp(dim));
            case ADVERSARIAL -> new PhaseWeights(adversarialMask(dim));
        };
    }

    @Test
    @DisplayName("IVF routing diagnostic: P1 coverage + P2 moment correlation + P3 scoring variants")
    void ivfRoutingDiagnostic() throws Exception {
        System.out.println();
        System.out.println("=".repeat(120));
        System.out.println("IVF Routing Diagnostic — Iteration 032 (P1 + P2 + P3 + P12 + P13)");
        System.out.printf("  N=%d, D=%d, queries=%d, topK=%d%n", N, DIM, NUM_QUERIES, TOP_K);
        System.out.println("=".repeat(120));

        Random dataRng = new Random(DATA_SEED);
        WavePattern[] dataPatterns = new WavePattern[N];
        for (int i = 0; i < N; i++) {
            dataPatterns[i] = randomPattern(dataRng, DIM);
        }

        int holdoutCount = NUM_QUERIES / 2;
        int tuningCount = NUM_QUERIES - holdoutCount;
        WavePattern[] allQueries = new WavePattern[NUM_QUERIES];
        Random queryRng = new Random(QUERY_SEED);
        for (int i = 0; i < NUM_QUERIES; i++) {
            allQueries[i] = randomPattern(queryRng, DIM);
        }
        WavePattern[] holdoutQueries = Arrays.copyOfRange(allQueries, tuningCount, NUM_QUERIES);

        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.delta.maxSize", String.valueOf(N + 1));
        System.setProperty("resonance.index.exactEquivalence", "false");

        Path storeDir = tempDir.resolve("ivf-diag");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            long t0 = System.nanoTime();
            String[] insertedIds = new String[N];
            int actualN = 0;
            for (int i = 0; i < N; i++) {
                try {
                    insertedIds[i] = store.insert(dataPatterns[i], Map.of());
                    actualN++;
                } catch (Exception e) { /* dup */ }
            }
            long insertMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("  Inserted: %d patterns in %d ms%n", actualN, insertMs);

            t0 = System.nanoTime();
            store.forceIndexRebuild();
            long buildMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("  Index build: %d ms%n", buildMs);

            Field ivfField = WavePatternStoreImpl.class.getDeclaredField("ivfSource");
            ivfField.setAccessible(true);
            IvfCandidateSource ivfSource = (IvfCandidateSource) ivfField.get(store);
            assertNotNull(ivfSource, "IVF source must be initialized");

            CentroidIndex centroidIndex = ivfSource.currentIndex();
            ResonanceMoments moments = ivfSource.currentMoments();
            assertNotNull(centroidIndex, "CentroidIndex must exist");
            assertNotNull(moments, "ResonanceMoments must exist");

            int K = centroidIndex.size();
            int unfoldedDim = centroidIndex.dim();
            double[][] centroids = centroidIndex.centroids();
            System.out.printf("  K=%d centroids, unfoldedDim=%d, lambda=%.2f%n",
                    K, unfoldedDim, centroidIndex.lambda());

            Map<String, Integer> patternToNearest = new HashMap<>();
            Map<Integer, Set<String>> sidecarPartitions = new HashMap<>();
            for (int c = 0; c < K; c++) {
                sidecarPartitions.put(c, new HashSet<>());
            }

            for (int i = 0; i < N; i++) {
                if (insertedIds[i] == null || dataPatterns[i] == null) continue;
                double[] u = UnfoldedMath.unfold(dataPatterns[i]);
                UnfoldedMath.l2Normalize(u);
                int nearest = nearestCentroid(u, centroids, unfoldedDim);
                patternToNearest.put(insertedIds[i], nearest);
                sidecarPartitions.get(nearest).add(insertedIds[i]);
            }

            int minSize = Integer.MAX_VALUE, maxSize = 0;
            double sumSize = 0;
            for (int c = 0; c < K; c++) {
                int s = sidecarPartitions.get(c).size();
                minSize = Math.min(minSize, s);
                maxSize = Math.max(maxSize, s);
                sumSize += s;
            }
            System.out.printf("  Cluster sizes: min=%d, max=%d, mean=%.1f, std=%.1f%n",
                    minSize, maxSize, sumSize / K, clusterStd(sidecarPartitions, K, sumSize / K));

            Map<String, Integer> idToIndex = new HashMap<>();
            for (int i = 0; i < N; i++) {
                if (insertedIds[i] != null) idToIndex.put(insertedIds[i], i);
            }

            ResonanceKernel kernel = new JavaKernel();

            System.out.println();
            printSection("P12: Exhaustive Scan Latency Baseline");

            for (int q = 0; q < Math.min(3, holdoutCount); q++) {
                for (int i = 0; i < actualN; i++) {
                    if (dataPatterns[i] != null) kernel.compare(holdoutQueries[q], dataPatterns[i]);
                }
            }

            long[] defaultExhLatencies = new long[holdoutCount];
            for (int q = 0; q < holdoutCount; q++) {
                long start = System.nanoTime();
                for (int i = 0; i < actualN; i++) {
                    if (dataPatterns[i] != null && insertedIds[i] != null)
                        kernel.compare(holdoutQueries[q], dataPatterns[i]);
                }
                defaultExhLatencies[q] = System.nanoTime() - start;
            }
            Arrays.sort(defaultExhLatencies);
            System.out.printf("  Default exhaustive: p50=%.1f ms, p95=%.1f ms, p99=%.1f ms%n",
                    defaultExhLatencies[holdoutCount / 2] / 1e6,
                    defaultExhLatencies[(int)(holdoutCount * 0.95)] / 1e6,
                    defaultExhLatencies[(int)(holdoutCount * 0.99)] / 1e6);

            CompareOptions sparseOpts = CompareOptions.withPhaseWeights(
                    new PhaseWeights(sparseMask(DIM, 0.10, new Random(MASK_SEED))));

            long[] sparseExhLatencies = new long[holdoutCount];
            for (int q = 0; q < holdoutCount; q++) {
                long start = System.nanoTime();
                for (int i = 0; i < actualN; i++) {
                    if (dataPatterns[i] != null && insertedIds[i] != null)
                        kernel.compare(holdoutQueries[q], dataPatterns[i], sparseOpts);
                }
                sparseExhLatencies[q] = System.nanoTime() - start;
            }
            Arrays.sort(sparseExhLatencies);
            System.out.printf("  Sparse exhaustive:  p50=%.1f ms, p95=%.1f ms, p99=%.1f ms%n",
                    sparseExhLatencies[holdoutCount / 2] / 1e6,
                    sparseExhLatencies[(int)(holdoutCount * 0.95)] / 1e6,
                    sparseExhLatencies[(int)(holdoutCount * 0.99)] / 1e6);

            printSection("P13: Default Path Recall (feature branch)");

            String[][] defaultGt10 = new String[holdoutCount][];
            for (int q = 0; q < holdoutCount; q++) {
                float[] scores = new float[actualN];
                for (int i = 0; i < actualN; i++) {
                    if (dataPatterns[i] != null && insertedIds[i] != null)
                        scores[i] = kernel.compare(holdoutQueries[q], dataPatterns[i]);
                }
                int[] sorted = partialSort(scores, actualN, TOP_K);
                defaultGt10[q] = new String[Math.min(TOP_K, sorted.length)];
                for (int i = 0; i < defaultGt10[q].length; i++)
                    defaultGt10[q][i] = insertedIds[sorted[i]];
            }

            int[] defaultProbeSweep = {4, 8, 12, 16, 24, 32};
            System.out.printf("  %-8s  %-8s  %-10s  %-8s  %-8s%n",
                    "nProbe", "K%", "Cand%", "R@10", "p50(ms)");
            System.out.println("  " + "-".repeat(50));

            for (int np : defaultProbeSweep) {
                if (np > K) continue;
                double totalR10 = 0, totalCandFrac = 0;
                long[] latencies = new long[holdoutCount];

                for (int q = 0; q < holdoutCount; q++) {
                    long start = System.nanoTime();
                    double[] queryU = UnfoldedMath.unfold(holdoutQueries[q]);
                    UnfoldedMath.l2Normalize(queryU);
                    int[] topP = rankCentroidsByCosine(queryU, centroids, unfoldedDim, np);

                    Set<String> candidates = new HashSet<>();
                    for (int partIdx : topP) candidates.addAll(sidecarPartitions.get(partIdx));
                    totalCandFrac += (double) candidates.size() / actualN;

                    List<ScoredId> scored = new ArrayList<>();
                    for (String id : candidates) {
                        Integer idx = idToIndex.get(id);
                        if (idx != null && dataPatterns[idx] != null)
                            scored.add(new ScoredId(id, kernel.compare(holdoutQueries[q], dataPatterns[idx])));
                    }
                    scored.sort((a, b) -> Float.compare(b.score, a.score));
                    latencies[q] = System.nanoTime() - start;

                    Set<String> top10 = topKIds(scored, TOP_K);
                    totalR10 += recall(defaultGt10[q], top10);
                }

                Arrays.sort(latencies);
                System.out.printf("  %-8d  %-8.1f  %-10.1f  %-8.4f  %-8.1f%n",
                        np, (double) np / K * 100, totalCandFrac / holdoutCount * 100,
                        totalR10 / holdoutCount, latencies[holdoutCount / 2] / 1e6);
            }

            double storeDefaultR10 = 0;
            for (int q = 0; q < holdoutCount; q++) {
                List<ResonanceMatch> res = store.query(holdoutQueries[q], TOP_K, null);
                Set<String> resIds = new HashSet<>();
                for (ResonanceMatch r : res) resIds.add(r.id());
                storeDefaultR10 += recall(defaultGt10[q], resIds);
            }
            storeDefaultR10 /= holdoutCount;
            System.out.printf("  store.query() default R@10 = %.4f%n", storeDefaultR10);
            int[] pctBudgets = {4, 8, 12, 16, 20, 24, 30, 40, 50};
            int[] absBudgets = new int[pctBudgets.length];
            for (int i = 0; i < pctBudgets.length; i++) {
                absBudgets[i] = Math.max(1, Math.min(K, (int) Math.ceil(K * pctBudgets[i] / 100.0)));
            }

            MaskType[] weightedMasks = {
                    MaskType.ALL_ZERO, MaskType.SPARSE, MaskType.DENSE,
                    MaskType.CONTINUOUS, MaskType.ADVERSARIAL
            };

            Random maskRng = new Random(MASK_SEED);
            for (MaskType mask : weightedMasks) {
                CompareOptions opts = optionsFor(mask, DIM, maskRng);
                PhaseWeights pw = (opts != null && opts.phaseWeights() != null)
                        ? opts.phaseWeights() : null;
                PhaseRoutingProfile profile = PhaseRoutingProfile.from(opts);

                printSection("P1/P2/P3: " + mask.label);
                System.out.printf("  drift=%.3f, suppress=%.3f, meanPart=%.3f%n",
                        profile.weightDrift(), 1.0 - profile.meanParticipation(),
                        profile.meanParticipation());

                int baseNProbe = 8;
                int adaptiveProbe = IvfCandidateSource.computeAdaptiveProbe(baseNProbe, K, profile);
                System.out.printf("  adaptiveProbe=%d (%.0f%% of K=%d)%n",
                        adaptiveProbe, (double) adaptiveProbe / K * 100, K);

                String[][] gt10 = new String[holdoutCount][];
                for (int q = 0; q < holdoutCount; q++) {
                    float[] scores = new float[actualN];
                    for (int i = 0; i < actualN; i++) {
                        if (dataPatterns[i] != null && insertedIds[i] != null)
                            scores[i] = (opts != null)
                                    ? kernel.compare(holdoutQueries[q], dataPatterns[i], opts)
                                    : kernel.compare(holdoutQueries[q], dataPatterns[i]);
                    }
                    int[] sorted = partialSort(scores, actualN, TOP_K);
                    gt10[q] = new String[Math.min(TOP_K, sorted.length)];
                    for (int i = 0; i < gt10[q].length; i++)
                        gt10[q][i] = insertedIds[sorted[i]];
                }

                List<Integer> allFirstRanksA = new ArrayList<>();
                List<Integer> allFirstRanksB = new ArrayList<>();
                List<Integer> allWorstRanksA = new ArrayList<>();
                List<Integer> allWorstRanksB = new ArrayList<>();
                List<Integer> allGtRanksA = new ArrayList<>();
                List<Integer> allGtRanksB = new ArrayList<>();
                double avgGtCentroidCount = 0;

                double[] routeACov = new double[absBudgets.length];
                double[] routeBCov = new double[absBudgets.length];
                double[] mergedCov = new double[absBudgets.length];
                double[] rrfCov = new double[absBudgets.length];
                double[] recallAtBudget = new double[absBudgets.length];
                double[] candFracAtBudget = new double[absBudgets.length];

                List<Double> momentScores = new ArrayList<>();
                List<Double> actualBestScores = new ArrayList<>();
                List<Double> actualMeanTop5Scores = new ArrayList<>();
                List<Integer> containsGtFlags = new ArrayList<>();

                for (int q = 0; q < holdoutCount; q++) {
                    WavePattern query = holdoutQueries[q];

                    Set<Integer> gtCentroids = new LinkedHashSet<>();
                    for (String gtId : gt10[q]) {
                        if (gtId != null) {
                            Integer c = patternToNearest.get(gtId);
                            if (c != null) gtCentroids.add(c);
                        }
                    }
                    avgGtCentroidCount += gtCentroids.size();

                    double[] queryU = UnfoldedMath.unfold(query);
                    UnfoldedMath.l2Normalize(queryU);
                    int[] routeA = rankCentroidsByCosine(queryU, centroids, unfoldedDim, K);
                    int[] routeB = moments.topCentroidsByMoment(query, pw, K);
                    int[] rrfRanking = rrfFusion(routeA, routeB, K);

                    Map<Integer, Integer> rankA = new HashMap<>();
                    for (int i = 0; i < routeA.length; i++) rankA.put(routeA[i], i + 1);
                    Map<Integer, Integer> rankB = new HashMap<>();
                    for (int i = 0; i < routeB.length; i++) rankB.put(routeB[i], i + 1);

                    int firstA = K + 1, firstB = K + 1;
                    int worstA = 0, worstB = 0;
                    for (int c : gtCentroids) {
                        int rA = rankA.getOrDefault(c, K + 1);
                        int rB = rankB.getOrDefault(c, K + 1);
                        firstA = Math.min(firstA, rA);
                        firstB = Math.min(firstB, rB);
                        worstA = Math.max(worstA, rA);
                        worstB = Math.max(worstB, rB);
                        allGtRanksA.add(rA);
                        allGtRanksB.add(rB);
                    }
                    if (!gtCentroids.isEmpty()) {
                        allFirstRanksA.add(firstA);
                        allFirstRanksB.add(firstB);
                        allWorstRanksA.add(worstA);
                        allWorstRanksB.add(worstB);
                    }

                    for (int bi = 0; bi < absBudgets.length; bi++) {
                        int b = absBudgets[bi];

                        routeACov[bi] += coverage(routeA, b, gtCentroids);
                        routeBCov[bi] += coverage(routeB, b, gtCentroids);
                        rrfCov[bi] += coverage(rrfRanking, b, gtCentroids);

                        Set<Integer> merged = new LinkedHashSet<>();
                        for (int i = 0; i < Math.min(b, routeA.length); i++) merged.add(routeA[i]);
                        for (int i = 0; i < Math.min(b, routeB.length); i++) merged.add(routeB[i]);
                        mergedCov[bi] += coverageSet(merged, gtCentroids);

                        Set<String> candidatesAtBudget = new HashSet<>();
                        for (int c : merged) candidatesAtBudget.addAll(sidecarPartitions.get(c));
                        candFracAtBudget[bi] += (double) candidatesAtBudget.size() / actualN;

                        List<ScoredId> scored = new ArrayList<>();
                        for (String id : candidatesAtBudget) {
                            Integer idx = idToIndex.get(id);
                            if (idx != null && dataPatterns[idx] != null) {
                                float s = (opts != null)
                                        ? kernel.compare(query, dataPatterns[idx], opts)
                                        : kernel.compare(query, dataPatterns[idx]);
                                scored.add(new ScoredId(id, s));
                            }
                        }
                        scored.sort((a, bx) -> Float.compare(bx.score, a.score));
                        recallAtBudget[bi] += recall(gt10[q], topKIds(scored, TOP_K));
                    }

                    Set<String> gtIdSet = new HashSet<>(Arrays.asList(gt10[q]));
                    for (int c = 0; c < K; c++) {
                        float mScore = moments.scoreCentroid(c, query, pw);
                        Set<String> partIds = sidecarPartitions.get(c);
                        if (partIds.isEmpty()) continue;

                        float bestInCentroid = Float.NEGATIVE_INFINITY;
                        float[] centroidScores = new float[partIds.size()];
                        int ci = 0;
                        boolean hasGt = false;
                        for (String id : partIds) {
                            Integer idx = idToIndex.get(id);
                            if (idx != null && dataPatterns[idx] != null) {
                                float s = (opts != null)
                                        ? kernel.compare(query, dataPatterns[idx], opts)
                                        : kernel.compare(query, dataPatterns[idx]);
                                centroidScores[ci++] = s;
                                if (s > bestInCentroid) bestInCentroid = s;
                                if (gtIdSet.contains(id)) hasGt = true;
                            }
                        }
                        if (ci == 0) continue;

                        Arrays.sort(centroidScores, 0, ci);
                        float meanTop5 = 0;
                        int top5Count = Math.min(5, ci);
                        for (int i = ci - top5Count; i < ci; i++) meanTop5 += centroidScores[i];
                        meanTop5 /= top5Count;

                        momentScores.add((double) mScore);
                        actualBestScores.add((double) bestInCentroid);
                        actualMeanTop5Scores.add((double) meanTop5);
                        containsGtFlags.add(hasGt ? 1 : 0);
                    }
                }

                avgGtCentroidCount /= holdoutCount;

                System.out.printf("  Mean GT centroids per query: %.1f / %d (%.1f%% of K)%n",
                        avgGtCentroidCount, K, avgGtCentroidCount / K * 100);

                System.out.println("\n  GT centroid rank statistics:");
                System.out.printf("    Route A — first GT rank: median=%d, p95=%d%n",
                        median(allFirstRanksA), percentile(allFirstRanksA, 95));
                System.out.printf("    Route A — worst GT rank: median=%d, p95=%d%n",
                        median(allWorstRanksA), percentile(allWorstRanksA, 95));
                System.out.printf("    Route A — all GT ranks:  median=%d, p95=%d%n",
                        median(allGtRanksA), percentile(allGtRanksA, 95));
                System.out.printf("    Route B — first GT rank: median=%d, p95=%d%n",
                        median(allFirstRanksB), percentile(allFirstRanksB, 95));
                System.out.printf("    Route B — worst GT rank: median=%d, p95=%d%n",
                        median(allWorstRanksB), percentile(allWorstRanksB, 95));
                System.out.printf("    Route B — all GT ranks:  median=%d, p95=%d%n",
                        median(allGtRanksB), percentile(allGtRanksB, 95));

                System.out.printf("%n  %-8s  %-8s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s%n",
                        "Budget", "K%", "RouteA", "RouteB", "Merged", "RRF", "CandFrac%", "R@10");
                System.out.println("  " + "-".repeat(85));

                for (int bi = 0; bi < absBudgets.length; bi++) {
                    System.out.printf("  %-8d  %-8d  %-10.4f  %-10.4f  %-10.4f  %-10.4f  %-10.1f  %-10.4f%n",
                            absBudgets[bi], pctBudgets[bi],
                            routeACov[bi] / holdoutCount,
                            routeBCov[bi] / holdoutCount,
                            mergedCov[bi] / holdoutCount,
                            rrfCov[bi] / holdoutCount,
                            candFracAtBudget[bi] / holdoutCount * 100,
                            recallAtBudget[bi] / holdoutCount);
                }

                System.out.println("\n  P2: Moment score correlation with actual centroid usefulness:");
                double corrBest = spearmanCorrelation(momentScores, actualBestScores);
                double corrMeanTop5 = spearmanCorrelation(momentScores, actualMeanTop5Scores);
                System.out.printf("    Spearman(moment, bestExactScore):    %.4f%n", corrBest);
                System.out.printf("    Spearman(moment, meanTop5Score):     %.4f%n", corrMeanTop5);

                double gtMomentSum = 0; int gtMomentCount = 0;
                double nonGtMomentSum = 0; int nonGtMomentCount = 0;
                for (int i = 0; i < containsGtFlags.size(); i++) {
                    if (containsGtFlags.get(i) == 1) {
                        gtMomentSum += momentScores.get(i);
                        gtMomentCount++;
                    } else {
                        nonGtMomentSum += momentScores.get(i);
                        nonGtMomentCount++;
                    }
                }
                System.out.printf("    Mean moment score (GT centroids):    %.6f (%d centroids)%n",
                        gtMomentCount > 0 ? gtMomentSum / gtMomentCount : 0, gtMomentCount);
                System.out.printf("    Mean moment score (non-GT centroids):%.6f (%d centroids)%n",
                        nonGtMomentCount > 0 ? nonGtMomentSum / nonGtMomentCount : 0, nonGtMomentCount);
                double ratio = (nonGtMomentCount > 0 && gtMomentCount > 0)
                        ? (gtMomentSum / gtMomentCount) / (nonGtMomentSum / nonGtMomentCount)
                        : 0;
                System.out.printf("    GT/non-GT moment ratio:              %.4f%n", ratio);

                System.out.println("\n  P3: Alternative moment scoring formulas — GT centroid rank comparison:");
                compareMomentFormulas(moments, holdoutQueries, holdoutCount, pw, K,
                        gt10, patternToNearest, absBudgets, pctBudgets);

                momentScores.clear();
                actualBestScores.clear();
                actualMeanTop5Scores.clear();
                containsGtFlags.clear();

                System.out.println("\n  Per-query GT centroid ranks (first 3):");
                for (int q = 0; q < Math.min(3, holdoutCount); q++) {
                    WavePattern query = holdoutQueries[q];
                    Set<Integer> gtCentroids = new LinkedHashSet<>();
                    for (String gtId : gt10[q]) {
                        if (gtId != null) {
                            Integer c = patternToNearest.get(gtId);
                            if (c != null) gtCentroids.add(c);
                        }
                    }
                    double[] queryU = UnfoldedMath.unfold(query);
                    UnfoldedMath.l2Normalize(queryU);
                    int[] routeA = rankCentroidsByCosine(queryU, centroids, unfoldedDim, K);
                    int[] routeB = moments.topCentroidsByMoment(query, pw, K);
                    Map<Integer, Integer> rankA = new HashMap<>();
                    for (int i = 0; i < routeA.length; i++) rankA.put(routeA[i], i + 1);
                    Map<Integer, Integer> rankB = new HashMap<>();
                    for (int i = 0; i < routeB.length; i++) rankB.put(routeB[i], i + 1);

                    StringBuilder sb = new StringBuilder();
                    sb.append(String.format("    q%d: %d GT centroids → ", q, gtCentroids.size()));
                    for (int c : gtCentroids) {
                        sb.append(String.format("c%d(rA=%d,rB=%d) ",
                                c, rankA.getOrDefault(c, K + 1), rankB.getOrDefault(c, K + 1)));
                    }
                    System.out.println(sb.toString().trim());
                }

                double storeWeightedR10 = 0;
                for (int q = 0; q < holdoutCount; q++) {
                    List<ResonanceMatch> res = store.query(holdoutQueries[q], TOP_K, opts);
                    Set<String> resIds = new HashSet<>();
                    for (ResonanceMatch r : res) resIds.add(r.id());
                    storeWeightedR10 += recall(gt10[q], resIds);
                }
                storeWeightedR10 /= holdoutCount;
                System.out.printf("  store.query() weighted R@10 = %.4f%n", storeWeightedR10);
            }

            System.out.println();
            printSection("DIAGNOSTIC COMPLETE");
            System.out.printf("  N=%d, D=%d, K=%d%n", actualN, DIM, K);

        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
            System.clearProperty("resonance.index.exactEquivalence");
            try { store.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    /**
     * Compares 4 moment scoring formulas for GT centroid ranking quality.
     *
     * A. Current (full resonance approximation with amplitude factor)
     * B. Cross-term only (raw cross interference, no normalization)
     * C. Normalized cross (cross / sqrt(Eq * Ec))
     * D. Amplitude-aware normalized (base only, no ampF)
     */
    private void compareMomentFormulas(ResonanceMoments moments,
                                        WavePattern[] queries, int queryCount,
                                        PhaseWeights pw, int K,
                                        String[][] gt10,
                                        Map<String, Integer> patternToNearest,
                                        int[] absBudgets, int[] pctBudgets) {

        int F = 4;
        String[] formulaNames = {"A.Current", "B.CrossOnly", "C.NormCross", "D.BaseOnly"};
        double[][] covAtBudget = new double[F][absBudgets.length];
        List<List<Integer>> allFirstRanks = new ArrayList<>();
        List<List<Integer>> allWorstRanks = new ArrayList<>();
        for (int f = 0; f < F; f++) {
            allFirstRanks.add(new ArrayList<>());
            allWorstRanks.add(new ArrayList<>());
        }

        for (int q = 0; q < queryCount; q++) {
            WavePattern query = queries[q];

            Set<Integer> gtCentroids = new LinkedHashSet<>();
            for (String gtId : gt10[q]) {
                if (gtId != null) {
                    Integer c = patternToNearest.get(gtId);
                    if (c != null) gtCentroids.add(c);
                }
            }
            if (gtCentroids.isEmpty()) continue;

            float[][] formulaScores = new float[F][K];
            double[] qA = query.amplitude();
            double[] qP = query.phase();
            int D = qA.length;
            double queryEnergy = 0;
            for (int i = 0; i < D; i++) queryEnergy += qA[i] * qA[i];

            for (int c = 0; c < K; c++) {
                formulaScores[0][c] = moments.scoreCentroid(c, query, pw);

                if (moments.count(c) == 0) continue;
                float[] mA = moments.meanAmplitude(c);
                float[] mR = moments.meanRealProj(c);
                float[] mI = moments.meanImagProj(c);
                float Ec = moments.meanEnergy(c);

                double cross = 0;
                if (pw == null || pw.isDefault()) {
                    for (int i = 0; i < D; i++) {
                        cross += qA[i] * (Math.cos(qP[i]) * mR[i] + Math.sin(qP[i]) * mI[i]);
                    }
                } else if (pw.isPhaseFree()) {
                    for (int i = 0; i < D; i++) {
                        cross += qA[i] * mA[i];
                    }
                } else {
                    double[] w = pw.rawWeights();
                    for (int i = 0; i < D; i++) {
                        double cosQ = Math.cos(qP[i]);
                        double sinQ = Math.sin(qP[i]);
                        double phaseTerm = cosQ * mR[i] + sinQ * mI[i];
                        cross += qA[i] * ((1.0 - w[i]) * mA[i] + w[i] * phaseTerm);
                    }
                }

                formulaScores[1][c] = (float) cross;

                double denom = Math.sqrt(queryEnergy * Ec);
                formulaScores[2][c] = (denom > 0) ? (float) (cross / denom) : 0;

                double denomFull = queryEnergy + Ec;
                formulaScores[3][c] = (denomFull > 0) ? (float) (0.5 * (queryEnergy + Ec + 2 * cross) / denomFull) : 0;
            }

            for (int f = 0; f < F; f++) {
                int[] ranking = rankByScore(formulaScores[f], K);
                Map<Integer, Integer> rankMap = new HashMap<>();
                for (int i = 0; i < ranking.length; i++) rankMap.put(ranking[i], i + 1);

                int first = K + 1, worst = 0;
                for (int c : gtCentroids) {
                    int r = rankMap.getOrDefault(c, K + 1);
                    first = Math.min(first, r);
                    worst = Math.max(worst, r);
                }
                allFirstRanks.get(f).add(first);
                allWorstRanks.get(f).add(worst);

                for (int bi = 0; bi < absBudgets.length; bi++) {
                    covAtBudget[f][bi] += coverage(ranking, absBudgets[bi], gtCentroids);
                }
            }
        }

        System.out.printf("    %-14s  %-14s  %-14s  %-14s  %-14s%n",
                "Formula", "medFirstRank", "medWorstRank", "cov@20%K", "cov@40%K");
        System.out.println("    " + "-".repeat(72));

        int idx20 = findBudgetIndex(pctBudgets, 20);
        int idx40 = findBudgetIndex(pctBudgets, 40);

        for (int f = 0; f < F; f++) {
            double c20 = (idx20 >= 0) ? covAtBudget[f][idx20] / queryCount : -1;
            double c40 = (idx40 >= 0) ? covAtBudget[f][idx40] / queryCount : -1;
            System.out.printf("    %-14s  %-14d  %-14d  %-14.4f  %-14.4f%n",
                    formulaNames[f],
                    median(allFirstRanks.get(f)),
                    median(allWorstRanks.get(f)),
                    c20, c40);
        }

        System.out.println("\n    GT coverage by formula at each budget:");
        StringBuilder header = new StringBuilder("    %-8s  %-6s");
        for (int f = 0; f < F; f++) header.append("  %-12s");
        System.out.printf(header + "%n", "Budget", "K%",
                formulaNames[0], formulaNames[1], formulaNames[2], formulaNames[3]);
        System.out.println("    " + "-".repeat(70));

        for (int bi = 0; bi < absBudgets.length; bi++) {
            System.out.printf("    %-8d  %-6d", absBudgets[bi], pctBudgets[bi]);
            for (int f = 0; f < F; f++) {
                System.out.printf("  %-12.4f", covAtBudget[f][bi] / queryCount);
            }
            System.out.println();
        }
    }

    private static int median(List<Integer> values) {
        if (values.isEmpty()) return 0;
        List<Integer> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        return sorted.get(sorted.size() / 2);
    }

    private static int percentile(List<Integer> values, int pct) {
        if (values.isEmpty()) return 0;
        List<Integer> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int idx = Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * pct / 100.0) - 1);
        return sorted.get(Math.max(0, idx));
    }

    private static double spearmanCorrelation(List<Double> x, List<Double> y) {
        int n = x.size();
        if (n < 3) return 0;

        double[] rankX = computeRanks(x);
        double[] rankY = computeRanks(y);

        double meanRX = 0, meanRY = 0;
        for (int i = 0; i < n; i++) {
            meanRX += rankX[i];
            meanRY += rankY[i];
        }
        meanRX /= n;
        meanRY /= n;

        double num = 0, denX = 0, denY = 0;
        for (int i = 0; i < n; i++) {
            double dx = rankX[i] - meanRX;
            double dy = rankY[i] - meanRY;
            num += dx * dy;
            denX += dx * dx;
            denY += dy * dy;
        }
        double den = Math.sqrt(denX * denY);
        return (den > 0) ? num / den : 0;
    }

    private static double[] computeRanks(List<Double> values) {
        int n = values.size();
        Integer[] indices = new Integer[n];
        for (int i = 0; i < n; i++) indices[i] = i;
        Arrays.sort(indices, (a, b) -> Double.compare(values.get(a), values.get(b)));
        double[] ranks = new double[n];
        int i = 0;
        while (i < n) {
            int j = i;
            while (j < n && values.get(indices[j]).equals(values.get(indices[i]))) j++;
            double avgRank = (i + 1.0 + j) / 2.0;
            for (int k = i; k < j; k++) ranks[indices[k]] = avgRank;
            i = j;
        }
        return ranks;
    }

    private static double clusterStd(Map<Integer, Set<String>> partitions, int K, double mean) {
        double sumSq = 0;
        for (int c = 0; c < K; c++) {
            double diff = partitions.get(c).size() - mean;
            sumSq += diff * diff;
        }
        return Math.sqrt(sumSq / K);
    }

    private static double coverage(int[] ranking, int budget, Set<Integer> gtCentroids) {
        if (gtCentroids.isEmpty()) return 1.0;
        int found = 0;
        for (int i = 0; i < Math.min(budget, ranking.length); i++) {
            if (gtCentroids.contains(ranking[i])) found++;
        }
        return (double) found / gtCentroids.size();
    }

    private static double coverageSet(Set<Integer> probed, Set<Integer> gtCentroids) {
        if (gtCentroids.isEmpty()) return 1.0;
        int found = 0;
        for (int c : gtCentroids) {
            if (probed.contains(c)) found++;
        }
        return (double) found / gtCentroids.size();
    }

    private static double recall(String[] gt, Set<String> resultIds) {
        if (gt.length == 0) return 1.0;
        int hits = 0;
        for (String g : gt) {
            if (g != null && resultIds.contains(g)) hits++;
        }
        return (double) hits / gt.length;
    }

    private static Set<String> topKIds(List<ScoredId> scored, int k) {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < Math.min(k, scored.size()); i++) ids.add(scored.get(i).id);
        return ids;
    }

    /** Reciprocal Rank Fusion of two rankings. */
    private static int[] rrfFusion(int[] rankA, int[] rankB, int K) {
        double rrf_k = 60.0;
        Map<Integer, Double> scores = new HashMap<>();
        for (int i = 0; i < rankA.length; i++) {
            scores.merge(rankA[i], 1.0 / (rrf_k + i + 1), Double::sum);
        }
        for (int i = 0; i < rankB.length; i++) {
            scores.merge(rankB[i], 1.0 / (rrf_k + i + 1), Double::sum);
        }
        return scores.entrySet().stream()
                .sorted(Map.Entry.<Integer, Double>comparingByValue().reversed())
                .mapToInt(Map.Entry::getKey)
                .toArray();
    }

    /** Ranks centroids by score descending, returns centroid indices. */
    private static int[] rankByScore(float[] scores, int K) {
        Integer[] indices = new Integer[K];
        for (int i = 0; i < K; i++) indices[i] = i;
        Arrays.sort(indices, (a, b) -> Float.compare(scores[b], scores[a]));
        int[] result = new int[K];
        for (int i = 0; i < K; i++) result[i] = indices[i];
        return result;
    }

    private static int findBudgetIndex(int[] pctBudgets, int target) {
        for (int i = 0; i < pctBudgets.length; i++) {
            if (pctBudgets[i] == target) return i;
        }
        int best = 0;
        for (int i = 1; i < pctBudgets.length; i++) {
            if (Math.abs(pctBudgets[i] - target) < Math.abs(pctBudgets[best] - target)) best = i;
        }
        return best;
    }

    private static void printSection(String title) {
        System.out.println();
        System.out.println("=".repeat(120));
        System.out.println("  " + title);
        System.out.println("=".repeat(120));
    }

    record ScoredId(String id, float score) {}

    private static int nearestCentroid(double[] point, double[][] centroids, int dim) {
        int best = 0;
        double bestDot = Double.NEGATIVE_INFINITY;
        for (int c = 0; c < centroids.length; c++) {
            double dot = 0.0;
            for (int d = 0; d < dim; d++) dot += point[d] * centroids[c][d];
            if (dot > bestDot) { bestDot = dot; best = c; }
        }
        return best;
    }

    private static int[] rankCentroidsByCosine(double[] normalizedQuery,
                                                double[][] centroids, int dim, int nProbe) {
        int k = centroids.length;
        nProbe = Math.min(nProbe, k);
        double[] dots = new double[k];
        for (int c = 0; c < k; c++) {
            double dot = 0.0;
            for (int d = 0; d < dim; d++) dot += normalizedQuery[d] * centroids[c][d];
            dots[c] = dot;
        }
        int[] indices = new int[k];
        for (int i = 0; i < k; i++) indices[i] = i;
        for (int i = 0; i < nProbe; i++) {
            int maxIdx = i;
            for (int j = i + 1; j < k; j++) {
                if (dots[indices[j]] > dots[indices[maxIdx]]) maxIdx = j;
            }
            int tmp = indices[i]; indices[i] = indices[maxIdx]; indices[maxIdx] = tmp;
        }
        return Arrays.copyOf(indices, nProbe);
    }

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static int[] partialSort(float[] scores, int n, int k) {
        k = Math.min(k, n);
        int[] idx = new int[n];
        for (int i = 0; i < n; i++) idx[i] = i;
        for (int i = 0; i < k; i++) {
            int best = i;
            for (int j = i + 1; j < n; j++) {
                if (scores[idx[j]] > scores[idx[best]]) best = j;
            }
            if (best != i) { int tmp = idx[i]; idx[i] = idx[best]; idx[best] = tmp; }
        }
        return Arrays.copyOf(idx, k);
    }

    private static double[] ones(int dim) {
        double[] w = new double[dim];
        Arrays.fill(w, 1.0);
        return w;
    }

    private static double[] sparseMask(int dim, double frac, Random rng) {
        double[] w = new double[dim];
        int active = Math.max(1, (int)(dim * frac));
        List<Integer> indices = new ArrayList<>(dim);
        for (int i = 0; i < dim; i++) indices.add(i);
        Collections.shuffle(indices, rng);
        for (int i = 0; i < active; i++) w[indices.get(i)] = 1.0;
        return w;
    }

    private static double[] denseMask(int dim, double frac, Random rng) {
        double[] w = new double[dim];
        Arrays.fill(w, 1.0);
        int inactive = (int)(dim * (1.0 - frac));
        List<Integer> indices = new ArrayList<>(dim);
        for (int i = 0; i < dim; i++) indices.add(i);
        Collections.shuffle(indices, rng);
        for (int i = 0; i < inactive; i++) w[indices.get(i)] = 0.0;
        return w;
    }

    private static double[] continuousRamp(int dim) {
        double[] w = new double[dim];
        double divisor = Math.max(1, dim - 1);
        for (int i = 0; i < dim; i++) w[i] = i / divisor;
        return w;
    }

    private static double[] adversarialMask(int dim) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++) w[i] = (i % 2 == 0) ? 1.0 : 0.0;
        return w;
    }
}
