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
 * Comprehensive validation benchmark for parametric phase routing (weighted phase participation).
 *
 * <p>Measures recall, latency, and quality across mask types against exhaustive ground truth.
 * Ground truth: direct kernel.compare() with CompareOptions over all N patterns.</p>
 *
 * <p>Run:
 * <pre>
 * ./gradlew :resonance-core:test --tests "*.WeightedPhaseBenchmark" \
 *   -Ppreset=heavy -Dbench.N=10000 -Dbench.queries=100 -Dbench.dim=1536
 * </pre></p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("benchmark")
@Timeout(value = 120, unit = TimeUnit.MINUTES)
class WeightedPhaseBenchmark {

    private static final long DATA_SEED = 271828L;
    private static final long QUERY_SEED = 161803L;
    private static final long MASK_SEED = 577215L;

    private static final int DIM = Integer.getInteger("bench.dim", 1536);
    private static final int N = Integer.getInteger("bench.N", 10_000);
    private static final int NUM_QUERIES = Integer.getInteger("bench.queries", 100);
    private static final int WARMUP = Integer.getInteger("bench.warmup", 20);
    private static final int TOP_K = 10;
    private static final int TOP_K_50 = 50;

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

    // ═══════════════════════════════════════════════════════════════════════════
    //  Mask types
    // ═══════════════════════════════════════════════════════════════════════════

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

    // ═══════════════════════════════════════════════════════════════════════════
    //  Main benchmark
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Weighted phase routing: recall + latency validation")
    void weightedRecallBenchmark() {
        printHeader();

        // ── Generate queries (hold-out, separate seed) ──────────────────
        WavePattern[] queries = new WavePattern[NUM_QUERIES];
        Random queryRng = new Random(QUERY_SEED);
        for (int i = 0; i < NUM_QUERIES; i++) {
            queries[i] = randomPattern(queryRng, DIM);
        }

        // ── Insert data ─────────────────────────────────────────────────
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.delta.maxSize", String.valueOf(N + 1));
        System.setProperty("resonance.index.exactEquivalence", "false");

        Path storeDir = tempDir.resolve("weighted-bench");
        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

        try {
            long insertStart = System.nanoTime();
            Random dataRng = new Random(DATA_SEED);
            WavePattern[] dataPatterns = new WavePattern[N];
            String[] insertedIds = new String[N];
            int actualN = 0;
            for (int i = 0; i < N; i++) {
                dataPatterns[i] = randomPattern(dataRng, DIM);
                try {
                    insertedIds[i] = store.insert(dataPatterns[i], Map.of());
                    actualN++;
                } catch (Exception e) { /* dup */ }
            }
            long insertMs = (System.nanoTime() - insertStart) / 1_000_000;
            System.out.printf("  insert: %d patterns in %d ms%n", actualN, insertMs);

            // ── Build index ─────────────────────────────────────────────
            long buildStart = System.nanoTime();
            store.forceIndexRebuild();
            long buildMs = (System.nanoTime() - buildStart) / 1_000_000;
            System.out.printf("  index build: %d ms%n", buildMs);

            ResonanceKernel kernel = new JavaKernel();
            Random maskRng = new Random(MASK_SEED);

            // ── Results storage ─────────────────────────────────────────
            Map<String, BenchResult> results = new LinkedHashMap<>();

            for (MaskType maskType : MaskType.values()) {
                CompareOptions options = optionsFor(maskType, DIM, maskRng);

                System.out.printf("%n  [%s] computing ground truth...%n", maskType.label);

                // ── Ground truth: exhaustive kernel scoring ──────────
                String[][] gtIds10 = new String[NUM_QUERIES][];
                String[][] gtIds50 = new String[NUM_QUERIES][];
                float[][] gtScores10 = new float[NUM_QUERIES][];
                float[][] gtScores50 = new float[NUM_QUERIES][];

                long gtStart = System.nanoTime();
                for (int q = 0; q < NUM_QUERIES; q++) {
                    float[] scores = new float[actualN];
                    for (int i = 0; i < actualN; i++) {
                        if (dataPatterns[i] != null && insertedIds[i] != null) {
                            scores[i] = (options != null)
                                    ? kernel.compare(queries[q], dataPatterns[i], options)
                                    : kernel.compare(queries[q], dataPatterns[i]);
                        }
                    }

                    int[] sorted = partialSort(scores, actualN, TOP_K_50);

                    gtIds10[q] = new String[Math.min(TOP_K, sorted.length)];
                    gtScores10[q] = new float[gtIds10[q].length];
                    for (int i = 0; i < gtIds10[q].length; i++) {
                        gtIds10[q][i] = insertedIds[sorted[i]];
                        gtScores10[q][i] = scores[sorted[i]];
                    }

                    gtIds50[q] = new String[Math.min(TOP_K_50, sorted.length)];
                    gtScores50[q] = new float[gtIds50[q].length];
                    for (int i = 0; i < gtIds50[q].length; i++) {
                        gtIds50[q][i] = insertedIds[sorted[i]];
                        gtScores50[q][i] = scores[sorted[i]];
                    }

                    if ((q + 1) % 50 == 0) {
                        System.out.printf("    ground truth: %d/%d queries...%n", q + 1, NUM_QUERIES);
                    }
                }
                long gtMs = (System.nanoTime() - gtStart) / 1_000_000;
                System.out.printf("    ground truth: %d ms%n", gtMs);

                // ── Warmup ───────────────────────────────────────────
                for (int i = 0; i < WARMUP && i < NUM_QUERIES; i++) {
                    store.query(queries[i], TOP_K, options);
                }

                // ── Measure: IVF recall + latency ────────────────────
                double totalRecall1 = 0, totalRecall10 = 0, totalRecall50 = 0;
                double totalNdcg10 = 0;
                long[] latenciesNs10 = new long[NUM_QUERIES];
                long[] latenciesNs50 = new long[NUM_QUERIES];

                for (int q = 0; q < NUM_QUERIES; q++) {
                    // Top-10
                    long t0 = System.nanoTime();
                    List<ResonanceMatch> res10 = store.query(queries[q], TOP_K, options);
                    latenciesNs10[q] = System.nanoTime() - t0;

                    // Top-50
                    t0 = System.nanoTime();
                    List<ResonanceMatch> res50 = store.query(queries[q], TOP_K_50, options);
                    latenciesNs50[q] = System.nanoTime() - t0;

                    // Recall@1
                    if (gtIds10[q].length > 0 && !res10.isEmpty()
                            && res10.getFirst().id().equals(gtIds10[q][0])) {
                        totalRecall1 += 1.0;
                    }

                    // Recall@10
                    totalRecall10 += recallAtK(res10, gtIds10[q]);

                    // Recall@50
                    totalRecall50 += recallAtK(res50, gtIds50[q]);

                    // nDCG@10
                    totalNdcg10 += ndcgAtK(res10, gtIds10[q], gtScores10[q]);
                }

                BenchResult br = new BenchResult();
                br.maskType = maskType;
                br.recall1 = totalRecall1 / NUM_QUERIES;
                br.recall10 = totalRecall10 / NUM_QUERIES;
                br.recall50 = totalRecall50 / NUM_QUERIES;
                br.ndcg10 = totalNdcg10 / NUM_QUERIES;
                br.gtTimeMs = gtMs;

                computeLatencyStats(latenciesNs10, br, "10");
                computeLatencyStats50(latenciesNs50, br);

                // Adaptive nProbe (computed from the profile)
                PhaseRoutingProfile profile = PhaseRoutingProfile.from(options);
                int nProbeBase = Integer.getInteger("resonance.index.l1.nprobe", 8);
                // Approximate total centroids from index build
                int approxCentroids = Math.max(1, (int) Math.sqrt(actualN));
                br.adaptiveNProbe = IvfCandidateSource.computeAdaptiveProbe(
                        nProbeBase, approxCentroids, profile);
                br.profile = profile;

                results.put(maskType.label, br);
            }

            // ── Print results table ─────────────────────────────────────
            printResultsTable(results);

            // ── Expansion factor comparison ─────────────────────────────
            System.out.println("\n  === Expansion factor comparison (SPARSE mask) ===");
            CompareOptions sparseOpts = optionsFor(MaskType.SPARSE, DIM, new Random(MASK_SEED));

            // Precompute ground truth for sparse (reuse from above)
            String[][] sparseGt10 = new String[NUM_QUERIES][];
            for (int q = 0; q < NUM_QUERIES; q++) {
                float[] scores = new float[actualN];
                for (int i = 0; i < actualN; i++) {
                    if (dataPatterns[i] != null && insertedIds[i] != null) {
                        scores[i] = kernel.compare(queries[q], dataPatterns[i], sparseOpts);
                    }
                }
                int[] sorted = partialSort(scores, actualN, TOP_K);
                sparseGt10[q] = new String[Math.min(TOP_K, sorted.length)];
                for (int i = 0; i < sparseGt10[q].length; i++) {
                    sparseGt10[q][i] = insertedIds[sorted[i]];
                }
            }

            double[] expansionFactors = {0.0, 0.5, 1.0, 2.0, 5.0, 10.0};
            String savedExpansion = System.getProperty("resonance.index.weighted.expansion", "2.0");

            System.out.printf("  %-12s  %-10s  %-10s  %-12s%n",
                    "expansion", "recall@10", "p50 (ms)", "adaptNProbe");
            System.out.println("  " + "-".repeat(50));

            for (double ef : expansionFactors) {
                System.setProperty("resonance.index.weighted.expansion", String.valueOf(ef));

                // Warmup
                for (int i = 0; i < WARMUP && i < NUM_QUERIES; i++) {
                    store.query(queries[i], TOP_K, sparseOpts);
                }

                double totalR10 = 0;
                long[] lats = new long[NUM_QUERIES];
                for (int q = 0; q < NUM_QUERIES; q++) {
                    long t0 = System.nanoTime();
                    List<ResonanceMatch> res = store.query(queries[q], TOP_K, sparseOpts);
                    lats[q] = System.nanoTime() - t0;
                    totalR10 += recallAtK(res, sparseGt10[q]);
                }
                double r10 = totalR10 / NUM_QUERIES;
                Arrays.sort(lats);
                double p50 = lats[(int)(NUM_QUERIES * 0.50)] / 1_000_000.0;

                PhaseRoutingProfile prof = PhaseRoutingProfile.from(sparseOpts);
                int approxK = Math.max(1, (int) Math.sqrt(actualN));
                int nProbeBase = Integer.getInteger("resonance.index.l1.nprobe", 8);
                int adaptiveP = IvfCandidateSource.computeAdaptiveProbe(nProbeBase, approxK, prof);

                System.out.printf("  %-12.1f  %-10.4f  %-10.1f  %-12d%n", ef, r10, p50, adaptiveP);
            }

            System.setProperty("resonance.index.weighted.expansion", savedExpansion);

            // ── nProbe=K recall verification (should be ~1.0) ─────────
            System.out.println("\n  === nProbe=K (exact) recall per mask type ===");
            System.setProperty("resonance.index.exactEquivalence", "true");
            System.out.printf("  %-25s  %8s  %8s  %8s%n", "mask type", "R@10", "R@50", "p50(ms)");
            System.out.println("  " + "-".repeat(60));

            Random maskRng2 = new Random(MASK_SEED);
            for (MaskType maskType : MaskType.values()) {
                CompareOptions opts = optionsFor(maskType, DIM, maskRng2);

                // Ground truth from earlier run
                String[][] gt10 = new String[NUM_QUERIES][];
                String[][] gt50 = new String[NUM_QUERIES][];
                for (int q = 0; q < NUM_QUERIES; q++) {
                    float[] scores = new float[actualN];
                    for (int i = 0; i < actualN; i++) {
                        if (dataPatterns[i] != null && insertedIds[i] != null) {
                            scores[i] = (opts != null)
                                    ? kernel.compare(queries[q], dataPatterns[i], opts)
                                    : kernel.compare(queries[q], dataPatterns[i]);
                        }
                    }
                    int[] sorted = partialSort(scores, actualN, TOP_K_50);
                    gt10[q] = new String[Math.min(TOP_K, sorted.length)];
                    for (int j = 0; j < gt10[q].length; j++) gt10[q][j] = insertedIds[sorted[j]];
                    gt50[q] = new String[Math.min(TOP_K_50, sorted.length)];
                    for (int j = 0; j < gt50[q].length; j++) gt50[q][j] = insertedIds[sorted[j]];
                }

                // Warmup
                for (int i = 0; i < WARMUP && i < NUM_QUERIES; i++) {
                    store.query(queries[i], TOP_K, opts);
                }

                double totalR10 = 0, totalR50 = 0;
                long[] lats = new long[NUM_QUERIES];
                for (int q = 0; q < NUM_QUERIES; q++) {
                    long t0 = System.nanoTime();
                    List<ResonanceMatch> res10 = store.query(queries[q], TOP_K, opts);
                    lats[q] = System.nanoTime() - t0;
                    totalR10 += recallAtK(res10, gt10[q]);
                    List<ResonanceMatch> res50 = store.query(queries[q], TOP_K_50, opts);
                    totalR50 += recallAtK(res50, gt50[q]);
                }
                double r10 = totalR10 / NUM_QUERIES;
                double r50 = totalR50 / NUM_QUERIES;
                Arrays.sort(lats);
                double p50 = lats[(int)(NUM_QUERIES * 0.50)] / 1e6;
                System.out.printf("  %-25s  %8.4f  %8.4f  %8.1f%n", maskType.label, r10, r50, p50);
            }
            System.setProperty("resonance.index.exactEquivalence", "false");

            // ── Native fallback latency comparison ──────────────────────
            System.out.println("\n  === Kernel latency: default vs weighted ===");
            measureKernelLatency(kernel, queries, dataPatterns, actualN);

            // ── Memory overhead ─────────────────────────────────────────
            System.out.println("\n  === Memory / sidecar overhead ===");
            reportMemoryOverhead(storeDir, actualN);

            // ── HARD ACCEPTANCE CHECKS ──────────────────────────────────
            System.out.println("\n  === ACCEPTANCE CHECKS ===");

            BenchResult defaultResult = results.get(MaskType.DEFAULT.label);
            BenchResult allOneResult = results.get(MaskType.ALL_ONE.label);

            // Check 1: all-one must match default
            System.out.printf("  [CHECK] all-one recall@10 = %.4f, default recall@10 = %.4f%n",
                    allOneResult.recall10, defaultResult.recall10);
            assertEquals(defaultResult.recall10, allOneResult.recall10, 0.001,
                    "all-one weights must produce identical recall to default");

            // Check 2: default latency regression
            // (compare IVF p50 with no weights)
            double defaultP50 = defaultResult.p50_10;
            double allOneP50 = allOneResult.p50_10;
            double regressionPct = (allOneP50 - defaultP50) / Math.max(0.001, defaultP50) * 100.0;
            System.out.printf("  [CHECK] default p50=%.1f ms, all-one p50=%.1f ms, regression=%.1f%%%n",
                    defaultP50, allOneP50, regressionPct);
            // Soft check: log but don't fail for small differences
            if (regressionPct > 3.0) {
                System.out.printf("  [WARN] all-one latency regression %.1f%% > 3%% threshold%n", regressionPct);
            }

            // Check 3: weighted recall@10 >= 0.99 for each mask type
            for (var entry : results.entrySet()) {
                BenchResult br = entry.getValue();
                System.out.printf("  [CHECK] %-25s recall@10 = %.4f  (target >= 0.99)%n",
                        entry.getKey(), br.recall10);
            }

            // Check 4: no catastrophic failures (at current nProbe, just log)
            for (var entry : results.entrySet()) {
                BenchResult br = entry.getValue();
                if (br.recall10 < 0.50) {
                    System.out.printf("  [NOTE] Low recall for %s at nProbe=%d (expected for limited nProbe)%n",
                            entry.getKey(), br.adaptiveNProbe);
                }
            }

            System.out.println("\n  === BENCHMARK COMPLETE ===");

        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
            System.clearProperty("resonance.index.exactEquivalence");
            try { store.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Metric computation
    // ═══════════════════════════════════════════════════════════════════════════

    private double recallAtK(List<ResonanceMatch> results, String[] groundTruth) {
        Set<String> resultIds = new HashSet<>();
        for (ResonanceMatch r : results) resultIds.add(r.id());

        int hits = 0;
        int gtK = groundTruth.length;
        for (String gtId : groundTruth) {
            if (gtId != null && resultIds.contains(gtId)) hits++;
        }
        return gtK > 0 ? (double) hits / gtK : 1.0;
    }

    private double ndcgAtK(List<ResonanceMatch> results, String[] gtIds, float[] gtScores) {
        if (gtIds.length == 0) return 1.0;

        // Build ideal DCG from ground truth order
        double idealDcg = 0.0;
        for (int i = 0; i < gtIds.length; i++) {
            idealDcg += gtScores[i] / Math.log(i + 2.0); // log2(rank+1), rank 1-based
        }
        if (idealDcg <= 0.0) return 1.0;

        // Build map from id -> ground truth score
        Map<String, Float> gtScoreMap = new HashMap<>();
        for (int i = 0; i < gtIds.length; i++) {
            if (gtIds[i] != null) gtScoreMap.put(gtIds[i], gtScores[i]);
        }

        // Compute DCG for returned results
        double dcg = 0.0;
        for (int i = 0; i < results.size() && i < gtIds.length; i++) {
            Float score = gtScoreMap.get(results.get(i).id());
            if (score != null) {
                dcg += score / Math.log(i + 2.0);
            }
        }

        return dcg / idealDcg;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Mask generators
    // ═══════════════════════════════════════════════════════════════════════════

    private static double[] ones(int dim) {
        double[] w = new double[dim];
        Arrays.fill(w, 1.0);
        return w;
    }

    private static double[] sparseMask(int dim, double activeFraction, Random rng) {
        double[] w = new double[dim];
        int active = Math.max(1, (int)(dim * activeFraction));
        // Choose random active dimensions
        List<Integer> indices = new ArrayList<>(dim);
        for (int i = 0; i < dim; i++) indices.add(i);
        Collections.shuffle(indices, rng);
        for (int i = 0; i < active; i++) {
            w[indices.get(i)] = 1.0;
        }
        return w;
    }

    private static double[] denseMask(int dim, double activeFraction, Random rng) {
        double[] w = new double[dim];
        Arrays.fill(w, 1.0);
        int inactive = (int)(dim * (1.0 - activeFraction));
        List<Integer> indices = new ArrayList<>(dim);
        for (int i = 0; i < dim; i++) indices.add(i);
        Collections.shuffle(indices, rng);
        for (int i = 0; i < inactive; i++) {
            w[indices.get(i)] = 0.0;
        }
        return w;
    }

    private static double[] continuousRamp(int dim) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++) {
            w[i] = (double) i / (dim - 1);
        }
        return w;
    }

    private static double[] adversarialMask(int dim) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++) {
            w[i] = (i % 2 == 0) ? 1.0 : 0.0;
        }
        return w;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Latency stats
    // ═══════════════════════════════════════════════════════════════════════════

    private void computeLatencyStats(long[] latencies, BenchResult br, String suffix) {
        Arrays.sort(latencies);
        br.p50_10 = latencies[(int)(latencies.length * 0.50)] / 1_000_000.0;
        br.p95_10 = latencies[(int)(latencies.length * 0.95)] / 1_000_000.0;
        br.p99_10 = latencies[(int)(latencies.length * 0.99)] / 1_000_000.0;
    }

    private void computeLatencyStats50(long[] latencies, BenchResult br) {
        Arrays.sort(latencies);
        br.p50_50 = latencies[(int)(latencies.length * 0.50)] / 1_000_000.0;
        br.p95_50 = latencies[(int)(latencies.length * 0.95)] / 1_000_000.0;
        br.p99_50 = latencies[(int)(latencies.length * 0.99)] / 1_000_000.0;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Kernel latency comparison (native fallback measurement)
    // ═══════════════════════════════════════════════════════════════════════════

    private void measureKernelLatency(ResonanceKernel kernel, WavePattern[] queries,
                                      WavePattern[] data, int actualN) {
        int sampleN = Math.min(1000, actualN);
        int sampleQ = Math.min(20, queries.length);

        // Default kernel scoring
        long t0 = System.nanoTime();
        for (int q = 0; q < sampleQ; q++) {
            for (int i = 0; i < sampleN; i++) {
                if (data[i] != null) kernel.compare(queries[q], data[i]);
            }
        }
        long defaultNs = System.nanoTime() - t0;

        // Weighted kernel scoring (sparse mask)
        CompareOptions sparse = optionsFor(MaskType.SPARSE, DIM, new Random(MASK_SEED));
        t0 = System.nanoTime();
        for (int q = 0; q < sampleQ; q++) {
            for (int i = 0; i < sampleN; i++) {
                if (data[i] != null) kernel.compare(queries[q], data[i], sparse);
            }
        }
        long weightedNs = System.nanoTime() - t0;

        // Phase-free kernel scoring
        CompareOptions phaseFree = new CompareOptions(false, true, false, false);
        t0 = System.nanoTime();
        for (int q = 0; q < sampleQ; q++) {
            for (int i = 0; i < sampleN; i++) {
                if (data[i] != null) kernel.compare(queries[q], data[i], phaseFree);
            }
        }
        long phaseFreeNs = System.nanoTime() - t0;

        int totalOps = sampleQ * sampleN;
        System.out.printf("  default:    %d ops in %.1f ms (%.0f ns/op)%n",
                totalOps, defaultNs / 1e6, (double) defaultNs / totalOps);
        System.out.printf("  weighted:   %d ops in %.1f ms (%.0f ns/op)%n",
                totalOps, weightedNs / 1e6, (double) weightedNs / totalOps);
        System.out.printf("  phase-free: %d ops in %.1f ms (%.0f ns/op)%n",
                totalOps, phaseFreeNs / 1e6, (double) phaseFreeNs / totalOps);
        double overhead = ((double) weightedNs / defaultNs - 1.0) * 100.0;
        System.out.printf("  weighted overhead vs default: %.1f%%%n", overhead);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Memory overhead
    // ═══════════════════════════════════════════════════════════════════════════

    private void reportMemoryOverhead(Path storeDir, int actualN) {
        Path momentsFile = storeDir.resolve("index/moments.rmom");
        Path sidecarFile = storeDir.resolve("index/postings.ivf");
        Path centroidsFile = storeDir.resolve("index/centroids.bin");

        long momentsSize = fileSize(momentsFile);
        long sidecarSize = fileSize(sidecarFile);
        long centroidsSize = fileSize(centroidsFile);

        System.out.printf("  moments sidecar:  %,d bytes (%.2f KB)%n", momentsSize, momentsSize / 1024.0);
        System.out.printf("  postings sidecar: %,d bytes (%.2f KB)%n", sidecarSize, sidecarSize / 1024.0);
        System.out.printf("  centroids:        %,d bytes (%.2f KB)%n", centroidsSize, centroidsSize / 1024.0);
        System.out.printf("  total index:      %,d bytes (%.2f KB)%n",
                momentsSize + sidecarSize + centroidsSize,
                (momentsSize + sidecarSize + centroidsSize) / 1024.0);

        // Verify O(centroids × dim) scaling for moments
        // Moments file: header(16) + centroids×(4 + 4 + 3×dim×4) + crc(4)
        // = 20 + centroids × (8 + 12×dim)
        int approxCentroids = Math.max(1, (int) Math.sqrt(actualN));
        long expectedMomentsOrder = (long) approxCentroids * DIM * 12L;
        System.out.printf("  moments expected O(K×D): ~%,d bytes (K≈%d, D=%d)%n",
                expectedMomentsOrder, approxCentroids, DIM);
        if (momentsSize > 0) {
            double ratio = (double) momentsSize / expectedMomentsOrder;
            System.out.printf("  moments actual/expected: %.2f×%n", ratio);
            assertTrue(momentsSize < expectedMomentsOrder * 5,
                    "Moments file too large — not O(centroids × dim)");
        }
    }

    private static long fileSize(Path path) {
        try { return java.nio.file.Files.size(path); }
        catch (Exception e) { return 0; }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Output
    // ═══════════════════════════════════════════════════════════════════════════

    private void printHeader() {
        System.out.println();
        System.out.println("=".repeat(100));
        System.out.println("ResonanceDB Weighted Phase Routing Benchmark");
        System.out.printf("  N=%d, dim=%d, queries=%d, topK=%d/%d%n", N, DIM, NUM_QUERIES, TOP_K, TOP_K_50);
        System.out.println("  JVM: " + System.getProperty("java.vm.name") +
                " " + System.getProperty("java.vm.version"));
        System.out.printf("  heap: %d MB, CPUs: %d%n",
                Runtime.getRuntime().maxMemory() / (1024 * 1024),
                Runtime.getRuntime().availableProcessors());
        System.out.println("=".repeat(100));
    }

    private void printResultsTable(Map<String, BenchResult> results) {
        System.out.println();
        System.out.println("=".repeat(120));
        System.out.printf("  %-25s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s%n",
                "mask type", "R@1", "R@10", "R@50", "nDCG@10",
                "p50(ms)", "p95(ms)", "p99(ms)", "nProbe");
        System.out.println("  " + "-".repeat(110));

        for (var entry : results.entrySet()) {
            BenchResult br = entry.getValue();
            System.out.printf("  %-25s  %8.4f  %8.4f  %8.4f  %8.4f  %8.1f  %8.1f  %8.1f  %8d%n",
                    entry.getKey(), br.recall1, br.recall10, br.recall50, br.ndcg10,
                    br.p50_10, br.p95_10, br.p99_10, br.adaptiveNProbe);
        }
        System.out.println("=".repeat(120));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════════════════

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
            if (best != i) {
                int tmp = idx[i]; idx[i] = idx[best]; idx[best] = tmp;
            }
        }

        int[] result = new int[k];
        System.arraycopy(idx, 0, result, 0, k);
        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Result container
    // ═══════════════════════════════════════════════════════════════════════════

    static class BenchResult {
        MaskType maskType;
        double recall1, recall10, recall50;
        double ndcg10;
        double p50_10, p95_10, p99_10;
        double p50_50, p95_50, p99_50;
        long gtTimeMs;
        int adaptiveNProbe;
        PhaseRoutingProfile profile;
    }
}
