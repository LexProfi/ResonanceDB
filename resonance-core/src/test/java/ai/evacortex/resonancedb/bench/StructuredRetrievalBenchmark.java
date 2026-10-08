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

/**
 * Iteration 033 — Structured Retrieval Validation and Hybrid Routing.
 *
 * <p>Validates Parametric Phase IVF on structured (clustered) waveform data
 * vs uniform random negative control.</p>
 *
 * <p>Run:
 * <pre>
 * ./gradlew :resonance-core:testJavaPerf --tests "*.StructuredRetrievalBenchmark" \
 *   -Dbench.N=10000 -Dbench.dim=1536 -Dbench.queries=50
 * </pre></p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("benchmark")
@Timeout(value = 180, unit = TimeUnit.MINUTES)
class StructuredRetrievalBenchmark {

    private static final int DIM = Integer.getInteger("bench.dim", 1536);
    private static final int N = Integer.getInteger("bench.N", 10_000);
    private static final int NUM_QUERIES = Integer.getInteger("bench.queries", 50);
    private static final int WARMUP = 10;
    private static final int TOP_K = 10;
    private static final int TOP_K_50 = 50;

    private static final long DATA_SEED = 314159L;
    private static final long QUERY_SEED = 271828L;
    private static final long MASK_SEED = 577215L;

    @TempDir Path tempDir;
    private StoreRuntimeServices runtime;

    @BeforeAll
    void initRuntime() { runtime = StoreRuntimeServices.fromSystemProperties(); }

    @AfterAll
    void closeRuntime() { if (runtime != null) runtime.close(); }
    enum MaskType {
        DEFAULT("default"),
        ALL_ONE("all-one"),
        ALL_ZERO("phase-free"),
        SPARSE("sparse-10%"),
        DENSE("dense-90%"),
        CONTINUOUS("continuous"),
        CONTIGUOUS("contiguous-30%"),
        ADVERSARIAL("adversarial");

        final String label;
        MaskType(String label) { this.label = label; }
    }

    private CompareOptions optionsFor(MaskType type, int dim, Random rng) {
        return switch (type) {
            case DEFAULT -> null;
            case ALL_ONE -> CompareOptions.withPhaseWeights(new PhaseWeights(fill(dim, 1.0)));
            case ALL_ZERO -> CompareOptions.withPhaseWeights(new PhaseWeights(new double[dim]));
            case SPARSE -> CompareOptions.withPhaseWeights(new PhaseWeights(sparseMask(dim, 0.10, rng)));
            case DENSE -> CompareOptions.withPhaseWeights(new PhaseWeights(denseMask(dim, 0.90, rng)));
            case CONTINUOUS -> CompareOptions.withPhaseWeights(new PhaseWeights(continuousRamp(dim)));
            case CONTIGUOUS -> CompareOptions.withPhaseWeights(new PhaseWeights(contiguousMask(dim, 0.30)));
            case ADVERSARIAL -> CompareOptions.withPhaseWeights(new PhaseWeights(adversarialMask(dim)));
        };
    }

    @Test
    @DisplayName("Iteration 033: Structured retrieval validation")
    void structuredRetrievalValidation() throws Exception {
        printHeader();

        System.out.println("\n" + "═".repeat(120));
        System.out.println("  CORPUS A: UNIFORM RANDOM (negative control)");
        System.out.println("═".repeat(120));

        StructuredCorpusGenerator.Corpus randomCorpus =
                StructuredCorpusGenerator.generateRandom(DIM, N, DATA_SEED);
        System.out.println("  " + randomCorpus);

        WavePattern[] randomQueries = selectQueriesFromCorpus(randomCorpus, NUM_QUERIES, QUERY_SEED);

        runCorpusBenchmark("RANDOM", randomCorpus, randomQueries);

        System.out.println("\n" + "═".repeat(120));
        System.out.println("  CORPUS B: SYNTHETIC STRUCTURED (Gaussian mixture)");
        System.out.println("═".repeat(120));

        StructuredCorpusGenerator.Config structuredConfig =
                StructuredCorpusGenerator.Config.standard(DIM, N, DATA_SEED);
        StructuredCorpusGenerator.Corpus structuredCorpus =
                StructuredCorpusGenerator.generate(structuredConfig);
        System.out.println("  " + structuredCorpus);
        printClusterStats(structuredCorpus);

        WavePattern[] structuredQueries = selectQueriesFromCorpus(structuredCorpus, NUM_QUERIES, QUERY_SEED);

        runCorpusBenchmark("STRUCTURED", structuredCorpus, structuredQueries);

        System.out.println("\n" + "═".repeat(120));
        System.out.println("  CORPUS C: REAL WAVEFORM DATA");
        System.out.println("═".repeat(120));
        System.out.println("  REAL CORPUS UNAVAILABLE IN CURRENT ENVIRONMENT");
        System.out.println("  (No EchoThesis/SenseMesh corpus found in test environment)");
        System.out.println("═".repeat(120));

        System.out.println("\n" + "═".repeat(120));
        System.out.println("  BENCHMARK COMPLETE");
        System.out.println("═".repeat(120));
    }

    private void runCorpusBenchmark(String corpusName,
                                    StructuredCorpusGenerator.Corpus corpus,
                                    WavePattern[] queries) throws Exception {
        WavePattern[] data = corpus.patterns();
        int actualN = data.length;

        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.delta.maxSize", String.valueOf(actualN + 1));
        System.setProperty("resonance.index.exactEquivalence", "false");

        Path storeDir = tempDir.resolve(corpusName.toLowerCase() + "-bench");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            long insertStart = System.nanoTime();
            String[] insertedIds = new String[actualN];
            int inserted = 0;
            for (int i = 0; i < actualN; i++) {
                try {
                    insertedIds[i] = store.insert(data[i], Map.of());
                    inserted++;
                } catch (Exception e) { /* dup */ }
            }
            System.out.printf("  insert: %d patterns in %d ms%n",
                    inserted, (System.nanoTime() - insertStart) / 1_000_000);

            long buildStart = System.nanoTime();
            store.forceIndexRebuild();
            long buildMs = (System.nanoTime() - buildStart) / 1_000_000;
            System.out.printf("  index build: %d ms%n", buildMs);

            sectionDefaultIvfLocality(corpusName, store, queries, data, insertedIds, inserted);

            sectionGtCentroidLocality(corpusName, store, queries, data, insertedIds, inserted);

            sectionMomentDiscrimination(corpusName, store, queries, data, insertedIds, inserted);

            sectionRoutingComparison(corpusName, store, queries, data, insertedIds, inserted);

        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
            System.clearProperty("resonance.index.exactEquivalence");
            try { store.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    private void sectionDefaultIvfLocality(String corpus, WavePatternStoreImpl store,
                                            WavePattern[] queries, WavePattern[] data,
                                            String[] ids, int n) throws Exception {
        System.out.printf("%n  ── P5: Default IVF Locality [%s] ──%n", corpus);

        IvfCandidateSource ivf = getIvfSource(store);
        if (ivf == null || ivf.currentIndex() == null) {
            System.out.println("  [SKIP] No IVF index available");
            return;
        }

        int K = ivf.currentIndex().size();
        int ivfDim = ivf.currentIndex().dim();

        int[] occupancy = new int[K];
        for (int i = 0; i < n; i++) {
            if (data[i] == null || ids[i] == null) continue;
            double[] u = UnfoldedMath.unfold(data[i]);
            UnfoldedMath.l2Normalize(u);
            int nearest = nearestCentroid(u, ivf.currentIndex().centroids(), ivfDim);
            occupancy[nearest]++;
        }
        int[] sorted = Arrays.copyOf(occupancy, K);
        Arrays.sort(sorted);
        System.out.printf("  K=%d centroids, occupancy: min=%d, max=%d, mean=%.1f, median=%d%n",
                K, sorted[0], sorted[K - 1], (double) n / K, sorted[K / 2]);

        ResonanceKernel kernel = new JavaKernel();
        int effectiveQueries = Math.min(queries.length, 30);

        System.out.printf("  %6s  %6s  %8s  %8s  %8s%n", "nProbe", "K%", "Cand%", "R@10", "p50(ms)");
        System.out.println("  " + "-".repeat(50));
        for (int nProbe : new int[]{4, 8, 12, 16, 24, 32, K / 2, K}) {
            int probe = Math.min(nProbe, K);
            double totalR10 = 0;
            long[] lats = new long[effectiveQueries];
            for (int q = 0; q < effectiveQueries; q++) {
                String[] gt10 = exhaustiveTopK(queries[q], data, ids, n, kernel, null, TOP_K);
                long t0 = System.nanoTime();
                List<ResonanceMatch> res = store.query(queries[q], TOP_K, null);
                lats[q] = System.nanoTime() - t0;
                totalR10 += recallAtK(res, gt10);
            }
            Arrays.sort(lats);
            double r10 = totalR10 / effectiveQueries;
            double p50 = lats[effectiveQueries / 2] / 1e6;
            double candPct = (double) probe / K * 100;
            System.out.printf("  %6d  %5.1f%%  %7.1f%%  %8.4f  %8.1f%n",
                    probe, (double) probe / K * 100, candPct, r10, p50);
        }
    }

    private void sectionGtCentroidLocality(String corpus, WavePatternStoreImpl store,
                                            WavePattern[] queries, WavePattern[] data,
                                            String[] ids, int n) throws Exception {
        System.out.printf("%n  ── P6: GT Centroid Locality [%s] ──%n", corpus);

        IvfCandidateSource ivf = getIvfSource(store);
        if (ivf == null || ivf.currentIndex() == null) return;

        int K = ivf.currentIndex().size();
        int ivfDim = ivf.currentIndex().dim();
        ResonanceKernel kernel = new JavaKernel();

        int[] patternCentroid = new int[n];
        for (int i = 0; i < n; i++) {
            if (data[i] == null || ids[i] == null) { patternCentroid[i] = -1; continue; }
            double[] u = UnfoldedMath.unfold(data[i]);
            UnfoldedMath.l2Normalize(u);
            patternCentroid[i] = nearestCentroid(u, ivf.currentIndex().centroids(), ivfDim);
        }

        Random maskRng = new Random(MASK_SEED);
        MaskType[] weightedMasks = {MaskType.DEFAULT, MaskType.SPARSE, MaskType.DENSE,
                MaskType.CONTINUOUS, MaskType.ADVERSARIAL};

        int effectiveQueries = Math.min(queries.length, 30);

        System.out.printf("  %-15s  %8s  %8s  %8s  %8s  %8s  %8s  %8s%n",
                "Mask", "GT-cents", "med", "p95", "cov@50%", "cov@90%", "cov@99%", "cov@100%");
        System.out.println("  " + "-".repeat(95));

        for (MaskType maskType : weightedMasks) {
            CompareOptions opts = optionsFor(maskType, DIM, maskRng);

            List<Integer> allGtCentCounts = new ArrayList<>();
            double[] coverageFor50 = new double[effectiveQueries];
            double[] coverageFor90 = new double[effectiveQueries];
            double[] coverageFor99 = new double[effectiveQueries];
            double[] coverageFor100 = new double[effectiveQueries];

            for (int q = 0; q < effectiveQueries; q++) {
                String[] gt10 = exhaustiveTopK(queries[q], data, ids, n, kernel, opts, TOP_K);
                Set<Integer> gtCentroids = new HashSet<>();
                for (String gtId : gt10) {
                    if (gtId == null) continue;
                    for (int i = 0; i < n; i++) {
                        if (gtId.equals(ids[i]) && patternCentroid[i] >= 0) {
                            gtCentroids.add(patternCentroid[i]);
                            break;
                        }
                    }
                }
                allGtCentCounts.add(gtCentroids.size());

                double[] queryU = UnfoldedMath.unfold(queries[q]);
                UnfoldedMath.l2Normalize(queryU);
                int[] routeA = rankCentroidsByCosine(
                        queryU, ivf.currentIndex().centroids(), ivfDim, K);

                int found = 0;
                int totalGt = gtCentroids.size();
                boolean found50 = false, found90 = false, found99 = false;
                for (int rank = 0; rank < K; rank++) {
                    if (gtCentroids.contains(routeA[rank])) found++;
                    double frac = totalGt > 0 ? (double) found / totalGt : 1.0;
                    if (!found50 && frac >= 0.50) { coverageFor50[q] = (double)(rank + 1) / K; found50 = true; }
                    if (!found90 && frac >= 0.90) { coverageFor90[q] = (double)(rank + 1) / K; found90 = true; }
                    if (!found99 && frac >= 0.99) { coverageFor99[q] = (double)(rank + 1) / K; found99 = true; }
                }
                coverageFor100[q] = totalGt > 0 ? 1.0 : 0;
                found = 0;
                for (int rank = 0; rank < K; rank++) {
                    if (gtCentroids.contains(routeA[rank])) found++;
                    if (found == totalGt) { coverageFor100[q] = (double)(rank + 1) / K; break; }
                }
                if (!found50) coverageFor50[q] = 1.0;
                if (!found90) coverageFor90[q] = 1.0;
                if (!found99) coverageFor99[q] = 1.0;
            }

            Collections.sort(allGtCentCounts);
            Arrays.sort(coverageFor50);
            Arrays.sort(coverageFor90);
            Arrays.sort(coverageFor99);
            Arrays.sort(coverageFor100);

            int medGtCents = allGtCentCounts.get(effectiveQueries / 2);
            int p95GtCents = allGtCentCounts.get((int)(effectiveQueries * 0.95));
            double medCov50 = coverageFor50[effectiveQueries / 2];
            double medCov90 = coverageFor90[effectiveQueries / 2];
            double medCov99 = coverageFor99[effectiveQueries / 2];
            double medCov100 = coverageFor100[effectiveQueries / 2];
            double meanGt = allGtCentCounts.stream().mapToInt(Integer::intValue).average().orElse(0);

            System.out.printf("  %-15s  %7.1f   %6d  %6d  %7.1f%%  %7.1f%%  %7.1f%%  %7.1f%%%n",
                    maskType.label, meanGt, medGtCents, p95GtCents,
                    medCov50 * 100, medCov90 * 100, medCov99 * 100, medCov100 * 100);
        }
    }

    private void sectionMomentDiscrimination(String corpus, WavePatternStoreImpl store,
                                              WavePattern[] queries, WavePattern[] data,
                                              String[] ids, int n) throws Exception {
        System.out.printf("%n  ── P7: Moment Discrimination [%s] ──%n", corpus);

        IvfCandidateSource ivf = getIvfSource(store);
        if (ivf == null || ivf.currentIndex() == null) return;

        ResonanceMoments mom = ivf.currentMoments();
        if (mom == null) {
            System.out.println("  [SKIP] No moments available");
            return;
        }

        int K = ivf.currentIndex().size();
        int ivfDim = ivf.currentIndex().dim();
        ResonanceKernel kernel = new JavaKernel();

        int[] patternCentroid = new int[n];
        for (int i = 0; i < n; i++) {
            if (data[i] == null || ids[i] == null) { patternCentroid[i] = -1; continue; }
            double[] u = UnfoldedMath.unfold(data[i]);
            UnfoldedMath.l2Normalize(u);
            patternCentroid[i] = nearestCentroid(u, ivf.currentIndex().centroids(), ivfDim);
        }

        Random maskRng = new Random(MASK_SEED);
        MaskType[] weightedMasks = {MaskType.SPARSE, MaskType.DENSE, MaskType.CONTINUOUS, MaskType.ADVERSARIAL};
        int effectiveQueries = Math.min(queries.length, 20);

        System.out.printf("  %-15s  %10s  %12s  %12s  %10s%n",
                "Mask", "GT/nonGT", "Spearman(best)", "Spearman(m5)", "Rank-med");
        System.out.println("  " + "-".repeat(75));

        for (MaskType maskType : weightedMasks) {
            CompareOptions opts = optionsFor(maskType, DIM, maskRng);
            PhaseWeights pw = (opts != null && opts.phaseWeights() != null) ? opts.phaseWeights() : null;

            double sumRatios = 0;
            double sumSpearmanBest = 0;
            List<Integer> firstGtRanks = new ArrayList<>();
            int queriesProcessed = 0;

            for (int q = 0; q < effectiveQueries; q++) {
                String[] gt10 = exhaustiveTopK(queries[q], data, ids, n, kernel, opts, TOP_K);
                Set<Integer> gtCentroids = new HashSet<>();
                for (String gtId : gt10) {
                    if (gtId == null) continue;
                    for (int i = 0; i < n; i++) {
                        if (gtId.equals(ids[i]) && patternCentroid[i] >= 0) {
                            gtCentroids.add(patternCentroid[i]);
                            break;
                        }
                    }
                }
                if (gtCentroids.isEmpty()) continue;

                float[] momentScores = new float[K];
                for (int c = 0; c < K; c++) {
                    momentScores[c] = mom.scoreCentroid(c, queries[q], pw);
                }

                double gtSum = 0, nonGtSum = 0;
                int gtCount = 0, nonGtCount = 0;
                for (int c = 0; c < K; c++) {
                    if (gtCentroids.contains(c)) { gtSum += momentScores[c]; gtCount++; }
                    else { nonGtSum += momentScores[c]; nonGtCount++; }
                }
                double gtMean = gtCount > 0 ? gtSum / gtCount : 0;
                double nonGtMean = nonGtCount > 0 ? nonGtSum / nonGtCount : 0;
                if (nonGtMean > 0) sumRatios += gtMean / nonGtMean;

                int[] momentRanking = partialSort(momentScores, K, K);
                for (int rank = 0; rank < K; rank++) {
                    if (gtCentroids.contains(momentRanking[rank])) {
                        firstGtRanks.add(rank + 1);
                        break;
                    }
                }

                float[] bestExactPerCentroid = new float[K];
                for (int i = 0; i < n; i++) {
                    if (ids[i] == null || patternCentroid[i] < 0) continue;
                    float score = opts != null
                            ? kernel.compare(queries[q], data[i], opts)
                            : kernel.compare(queries[q], data[i]);
                    bestExactPerCentroid[patternCentroid[i]] =
                            Math.max(bestExactPerCentroid[patternCentroid[i]], score);
                }
                sumSpearmanBest += spearman(momentScores, bestExactPerCentroid, K);

                queriesProcessed++;
            }

            if (queriesProcessed == 0) continue;

            Collections.sort(firstGtRanks);
            int medRank = firstGtRanks.isEmpty() ? K : firstGtRanks.get(firstGtRanks.size() / 2);

            System.out.printf("  %-15s  %10.4f  %12.4f  %12s  %10d%n",
                    maskType.label,
                    sumRatios / queriesProcessed,
                    sumSpearmanBest / queriesProcessed,
                    "—",
                    medRank);
        }
    }

    private void sectionRoutingComparison(String corpus, WavePatternStoreImpl store,
                                           WavePattern[] queries, WavePattern[] data,
                                           String[] ids, int n) throws Exception {
        System.out.printf("%n  ── P8+P14: Routing Comparison + Performance [%s] ──%n", corpus);

        ResonanceKernel kernel = new JavaKernel();
        int effectiveQueries = Math.min(queries.length, NUM_QUERIES);

        Random maskRng = new Random(MASK_SEED);
        MaskType[] allMasks = MaskType.values();

        System.out.println("\n  Exhaustive baseline:");
        Field eqField = WavePatternStoreImpl.class.getDeclaredField("exactEquivalence");
        eqField.setAccessible(true);

        eqField.set(store, true);
        for (MaskType maskType : new MaskType[]{MaskType.DEFAULT, MaskType.SPARSE, MaskType.DENSE}) {
            CompareOptions opts = optionsFor(maskType, DIM, new Random(MASK_SEED));
            long[] lats = new long[effectiveQueries];
            for (int q = 0; q < effectiveQueries; q++) {
                long t0 = System.nanoTime();
                store.query(queries[q], TOP_K, opts);
                lats[q] = System.nanoTime() - t0;
            }
            Arrays.sort(lats);
            System.out.printf("    %-15s exhaustive p50=%.1f ms, p95=%.1f ms%n",
                    maskType.label,
                    lats[effectiveQueries / 2] / 1e6,
                    lats[(int)(effectiveQueries * 0.95)] / 1e6);
        }
        eqField.set(store, false);

        System.out.printf("%n  %-15s  %6s  %8s  %8s  %8s  %8s  %8s  %8s%n",
                "Mask", "nProbe", "R@1", "R@10", "R@50", "nDCG@10", "p50(ms)", "p95(ms)");
        System.out.println("  " + "-".repeat(85));

        Map<String, double[]> resultMap = new LinkedHashMap<>();

        for (MaskType maskType : allMasks) {
            CompareOptions opts = optionsFor(maskType, DIM, maskRng);

            for (int i = 0; i < WARMUP && i < effectiveQueries; i++) {
                store.query(queries[i], TOP_K, opts);
            }

            double totalR1 = 0, totalR10 = 0, totalR50 = 0, totalNdcg = 0;
            long[] lats10 = new long[effectiveQueries];

            for (int q = 0; q < effectiveQueries; q++) {
                String[] gt10 = exhaustiveTopK(queries[q], data, ids, n, kernel, opts, TOP_K);
                String[] gt50 = exhaustiveTopK(queries[q], data, ids, n, kernel, opts, TOP_K_50);

                long t0 = System.nanoTime();
                List<ResonanceMatch> res10 = store.query(queries[q], TOP_K, opts);
                lats10[q] = System.nanoTime() - t0;
                List<ResonanceMatch> res50 = store.query(queries[q], TOP_K_50, opts);

                if (!res10.isEmpty() && gt10.length > 0 && res10.getFirst().id().equals(gt10[0]))
                    totalR1 += 1.0;
                totalR10 += recallAtK(res10, gt10);
                totalR50 += recallAtK(res50, gt50);
                totalNdcg += ndcgAtK(res10, gt10);
            }

            Arrays.sort(lats10);
            double r1 = totalR1 / effectiveQueries;
            double r10 = totalR10 / effectiveQueries;
            double r50 = totalR50 / effectiveQueries;
            double ndcg = totalNdcg / effectiveQueries;
            double p50 = lats10[effectiveQueries / 2] / 1e6;
            double p95 = lats10[(int)(effectiveQueries * 0.95)] / 1e6;

            PhaseRoutingProfile profile = PhaseRoutingProfile.from(opts);
            int nProbeBase = Integer.getInteger("resonance.index.l1.nprobe", 8);
            int approxK = Math.max(1, (int) Math.sqrt(n));
            int adaptiveP = IvfCandidateSource.computeAdaptiveProbe(nProbeBase, approxK, profile);

            System.out.printf("  %-15s  %6d  %8.4f  %8.4f  %8.4f  %8.4f  %8.1f  %8.1f%n",
                    maskType.label, adaptiveP, r1, r10, r50, ndcg, p50, p95);

            resultMap.put(maskType.label, new double[]{r1, r10, r50, ndcg, p50, p95});
        }

        System.out.printf("%n  Acceptance gates [%s]:%n", corpus);
        for (var entry : resultMap.entrySet()) {
            double r10 = entry.getValue()[1];
            String status = r10 >= 0.99 ? "PASS" : "FAIL";
            System.out.printf("    %-15s R@10=%.4f  [%s]%n", entry.getKey(), r10, status);
        }
    }

    private String[] exhaustiveTopK(WavePattern query, WavePattern[] data,
                                     String[] ids, int n,
                                     ResonanceKernel kernel, CompareOptions opts, int topK) {
        float[] scores = new float[n];
        for (int i = 0; i < n; i++) {
            if (data[i] != null && ids[i] != null) {
                scores[i] = opts != null
                        ? kernel.compare(query, data[i], opts)
                        : kernel.compare(query, data[i]);
            }
        }
        int[] sorted = partialSort(scores, n, topK);
        String[] result = new String[Math.min(topK, sorted.length)];
        for (int i = 0; i < result.length; i++) result[i] = ids[sorted[i]];
        return result;
    }

    private double recallAtK(List<ResonanceMatch> results, String[] gt) {
        Set<String> resultIds = new HashSet<>();
        for (ResonanceMatch r : results) resultIds.add(r.id());
        int hits = 0;
        for (String gtId : gt) {
            if (gtId != null && resultIds.contains(gtId)) hits++;
        }
        return gt.length > 0 ? (double) hits / gt.length : 1.0;
    }

    private double ndcgAtK(List<ResonanceMatch> results, String[] gtIds) {
        if (gtIds.length == 0) return 1.0;
        double idealDcg = 0;
        for (int i = 0; i < gtIds.length; i++) idealDcg += 1.0 / Math.log(i + 2.0);
        if (idealDcg <= 0) return 1.0;

        Set<String> gtSet = new HashSet<>(Arrays.asList(gtIds));
        double dcg = 0;
        for (int i = 0; i < results.size() && i < gtIds.length; i++) {
            if (gtSet.contains(results.get(i).id())) dcg += 1.0 / Math.log(i + 2.0);
        }
        return dcg / idealDcg;
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

    private static double spearman(float[] a, float[] b, int n) {
        double[] rankA = ranks(a, n);
        double[] rankB = ranks(b, n);
        double sumD2 = 0;
        for (int i = 0; i < n; i++) {
            double d = rankA[i] - rankB[i];
            sumD2 += d * d;
        }
        return 1.0 - 6.0 * sumD2 / (n * ((long) n * n - 1));
    }

    private static double[] ranks(float[] values, int n) {
        Integer[] idx = new Integer[n];
        for (int i = 0; i < n; i++) idx[i] = i;
        Arrays.sort(idx, (a, b) -> Float.compare(values[b], values[a]));
        double[] r = new double[n];
        int i = 0;
        while (i < n) {
            int j = i;
            while (j < n && values[idx[j]] == values[idx[i]]) j++;
            double avgRank = (i + 1.0 + j) / 2.0;
            for (int k = i; k < j; k++) r[idx[k]] = avgRank;
            i = j;
        }
        return r;
    }

    private WavePattern[] selectQueriesFromCorpus(StructuredCorpusGenerator.Corpus corpus,
                                                    int numQueries, long seed) {
        Random rng = new Random(seed);
        WavePattern[] data = corpus.patterns();
        int n = data.length;
        WavePattern[] queries = new WavePattern[numQueries];

        for (int q = 0; q < numQueries; q++) {
            int baseIdx = rng.nextInt(n);
            WavePattern base = data[baseIdx];
            double[] amp = new double[base.amplitude().length];
            double[] phase = new double[base.phase().length];
            for (int d = 0; d < amp.length; d++) {
                amp[d] = Math.max(0.001, base.amplitude()[d] + rng.nextGaussian() * 0.05);
                phase[d] = base.phase()[d] + rng.nextGaussian() * 0.1;
                phase[d] = ((phase[d] % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI);
            }
            queries[q] = new WavePattern(amp, phase);
        }
        return queries;
    }

    private IvfCandidateSource getIvfSource(WavePatternStoreImpl store) throws Exception {
        for (Field f : WavePatternStoreImpl.class.getDeclaredFields()) {
            if (f.getType() == IvfCandidateSource.class) {
                f.setAccessible(true);
                return (IvfCandidateSource) f.get(store);
            }
        }
        return null;
    }

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

    private static double[] fill(int dim, double value) {
        double[] w = new double[dim];
        Arrays.fill(w, value);
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

    private static double[] contiguousMask(int dim, double frac) {
        double[] w = new double[dim];
        int active = Math.max(1, (int)(dim * frac));
        int start = dim / 4;
        for (int i = start; i < start + active && i < dim; i++) w[i] = 1.0;
        return w;
    }

    private static double[] adversarialMask(int dim) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++) w[i] = (i % 2 == 0) ? 1.0 : 0.0;
        return w;
    }

    private void printHeader() {
        System.out.println();
        System.out.println("═".repeat(120));
        System.out.println("  Iteration 033 — Structured Retrieval Validation and Hybrid Routing");
        System.out.printf("  N=%d, dim=%d, queries=%d, topK=%d/%d%n", N, DIM, NUM_QUERIES, TOP_K, TOP_K_50);
        System.out.printf("  heap: %d MB, CPUs: %d%n",
                Runtime.getRuntime().maxMemory() / (1024 * 1024),
                Runtime.getRuntime().availableProcessors());
        System.out.println("═".repeat(120));
    }

    private void printClusterStats(StructuredCorpusGenerator.Corpus corpus) {
        int[] sizes = corpus.clusterSizes();
        int[] sorted = Arrays.copyOf(sizes, sizes.length);
        Arrays.sort(sorted);
        int K = sizes.length;
        double mean = (double) corpus.patterns().length / K;
        System.out.printf("  K_gen=%d clusters, sizes: min=%d, max=%d, mean=%.1f, median=%d%n",
                K, sorted[0], sorted[K - 1], mean, sorted[K / 2]);
    }
}
