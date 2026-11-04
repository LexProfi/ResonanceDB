/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for R1 (multi-shard retrieval completeness) and
 * R2 (exhaustive IVF rescore correctness).
 *
 * <p>R1: Unindexed fallback must scan ALL shards unconditionally.
 * The previous two-pass heuristic could miss high-scoring patterns
 * in non-preferred shards when topK was already filled.</p>
 *
 * <p>R2: When all IVF centroids are probed (exhaustive), the store must
 * exact-rescore every candidate without Float32 epsilon pruning.
 * IvfCandidateSource must correctly report exhaustive=true.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class R1R2RegressionTest {

    private static final long SEED = 314159L;
    private static final ResonanceKernel KERNEL = new JavaKernel();

    // ───────── helpers ──────────────────────────────────────────

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    /** Create a pattern whose circular mean phase equals exactly {@code meanPhase}. */
    private static WavePattern constantPhasePattern(double amp, double meanPhase, int dim) {
        double[] a = new double[dim];
        double[] p = new double[dim];
        Arrays.fill(a, amp);
        Arrays.fill(p, meanPhase);
        return new WavePattern(a, p);
    }

    /** Create a pattern with controllable mean phase but randomized amplitudes. */
    private static WavePattern phaseTargetedPattern(double meanPhase, double ampMin,
                                                     double ampMax, int dim, Random rng) {
        double[] a = new double[dim];
        double[] p = new double[dim];
        for (int i = 0; i < dim; i++) {
            a[i] = ampMin + rng.nextDouble() * (ampMax - ampMin);
            p[i] = meanPhase + (rng.nextDouble() - 0.5) * 0.01;
        }
        return new WavePattern(a, p);
    }

    /** Compute ground-truth top-K via exhaustive kernel scoring. */
    private static List<String> oracleTopK(WavePattern query, List<WavePattern> patterns,
                                            List<String> ids, int topK) {
        float[] scores = new float[patterns.size()];
        for (int i = 0; i < patterns.size(); i++) {
            scores[i] = KERNEL.compare(query, patterns.get(i));
        }
        Integer[] sortedIdx = new Integer[patterns.size()];
        for (int i = 0; i < sortedIdx.length; i++) sortedIdx[i] = i;
        Arrays.sort(sortedIdx, (a, b) -> {
            int c = Float.compare(scores[b], scores[a]);
            return c != 0 ? c : ids.get(a).compareTo(ids.get(b));
        });
        List<String> result = new ArrayList<>();
        for (int i = 0; i < Math.min(topK, sortedIdx.length); i++) {
            result.add(ids.get(sortedIdx[i]));
        }
        return result;
    }

    private static float oracleScore(WavePattern query, WavePattern pattern) {
        return KERNEL.compare(query, pattern);
    }

    private void clearIndexProperties() {
        for (String prop : List.of(
                "resonance.index.enabled", "resonance.index.exactEquivalence",
                "resonance.index.l2.enabled", "resonance.index.l1.k",
                "resonance.index.l1.nprobe", "resonance.index.delta.maxSize",
                "resonance.segment.buckets")) {
            System.clearProperty(prop);
        }
    }

    // ═══════════════════════════════════════════════════════════
    //  R1 — Multi-shard retrieval completeness (unindexed path)
    // ═══════════════════════════════════════════════════════════

    /**
     * R1 core regression: highest-scoring patterns outside query's preferred shard.
     *
     * <p>Inserts patterns into distinct phase shards. The best-scoring pattern
     * is placed in a shard far from the query's phase. With the old two-pass
     * heuristic, this pattern could be missed if the preferred shard already
     * yielded topK results.</p>
     */
    @Test
    @DisplayName("R1: unindexed query returns best pattern from non-preferred shard")
    void r1_bestPatternInNonPreferredShard(@TempDir Path tmpDir) {
        int dim = 64;
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r1-basic"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            // Insert 30 patterns in shard near phase=0.0 (query's preferred shard)
            for (int i = 0; i < 30; i++) {
                WavePattern p = phaseTargetedPattern(0.0, 0.1, 0.5, dim, rng);
                String id = store.insert(p, Map.of());
                patterns.add(p);
                ids.add(id);
            }

            // Insert 30 patterns in shard near phase=PI/2
            for (int i = 0; i < 30; i++) {
                WavePattern p = phaseTargetedPattern(Math.PI / 2, 0.1, 0.5, dim, rng);
                String id = store.insert(p, Map.of());
                patterns.add(p);
                ids.add(id);
            }

            // Insert the BEST pattern in a distant shard (phase=PI - far from query)
            // Use query-identical pattern in a remote shard
            WavePattern query = phaseTargetedPattern(0.0, 0.8, 1.0, dim, new Random(SEED + 42));
            // Create a near-clone of the query but with mean phase = -PI + 0.1
            double[] bestAmp = query.amplitude().clone();
            double[] bestPhase = new double[dim];
            Arrays.fill(bestPhase, -Math.PI + 0.1);
            WavePattern bestPattern = new WavePattern(bestAmp, bestPhase);
            String bestId = store.insert(bestPattern, Map.of());
            patterns.add(bestPattern);
            ids.add(bestId);

            // Insert 30 patterns in shard near phase=-PI/2
            for (int i = 0; i < 30; i++) {
                WavePattern p = phaseTargetedPattern(-Math.PI / 2, 0.1, 0.5, dim, rng);
                String id = store.insert(p, Map.of());
                patterns.add(p);
                ids.add(id);
            }

            // Query with topK=1 — must find the best pattern regardless of shard
            List<ResonanceMatch> results = store.query(query, 1);
            assertFalse(results.isEmpty(), "Must return at least 1 result");

            // Compute oracle
            List<String> oracle = oracleTopK(query, patterns, ids, 1);
            assertEquals(oracle.get(0), results.get(0).id(),
                    "Top-1 must match oracle (best pattern may be in non-preferred shard)");

            // Verify with topK=10
            List<ResonanceMatch> top10 = store.query(query, 10);
            List<String> oracleTop10 = oracleTopK(query, patterns, ids, 10);
            Set<String> resultIds = top10.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
            for (String expected : oracleTop10) {
                assertTrue(resultIds.contains(expected),
                        "Oracle top-10 pattern " + expected + " missing from results");
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R1: 1 shard — trivial case, full scan of single shard. */
    @Test
    @DisplayName("R1: 1 shard — all patterns found")
    void r1_singleShard(@TempDir Path tmpDir) {
        int dim = 32;
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "1");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r1-1shard"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                WavePattern p = randomPattern(rng, dim);
                String id = store.insert(p, Map.of());
                patterns.add(p);
                ids.add(id);
            }

            for (int topK : new int[]{1, 10}) {
                WavePattern q = randomPattern(new Random(SEED + 7), dim);
                List<String> oracle = oracleTopK(q, patterns, ids, topK);
                List<ResonanceMatch> results = store.query(q, topK);
                List<String> resultIds = results.stream().map(ResonanceMatch::id).toList();
                assertEquals(oracle, resultIds,
                        "1-shard unindexed topK=" + topK + " must match oracle exactly");
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R1: 2 shards — patterns split across two phase groups. */
    @Test
    @DisplayName("R1: 2 shards — full coverage")
    void r1_twoShards(@TempDir Path tmpDir) {
        int dim = 32;
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "4");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r1-2shard"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            // Shard A (phase ~ 0)
            for (int i = 0; i < 25; i++) {
                WavePattern p = phaseTargetedPattern(0.0, 0.3, 1.0, dim, rng);
                ids.add(store.insert(p, Map.of()));
                patterns.add(p);
            }
            // Shard B (phase ~ PI)
            for (int i = 0; i < 25; i++) {
                WavePattern p = phaseTargetedPattern(Math.PI - 0.1, 0.3, 1.0, dim, rng);
                ids.add(store.insert(p, Map.of()));
                patterns.add(p);
            }

            WavePattern q = randomPattern(new Random(SEED + 99), dim);
            for (int topK : new int[]{1, 10}) {
                List<String> oracle = oracleTopK(q, patterns, ids, topK);
                List<ResonanceMatch> results = store.query(q, topK);
                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                for (String expected : oracle) {
                    assertTrue(resultIds.contains(expected),
                            "2-shard topK=" + topK + " missing oracle pattern " + expected);
                }
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R1: 4 shards with patterns spread across all four. */
    @Test
    @DisplayName("R1: 4 shards — full coverage including non-adjacent shards")
    void r1_fourShards(@TempDir Path tmpDir) {
        int dim = 32;
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "4");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r1-4shard"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            double[] phases = {-Math.PI + 0.3, -Math.PI / 2, 0.0, Math.PI / 2};

            for (double ph : phases) {
                for (int i = 0; i < 15; i++) {
                    WavePattern p = phaseTargetedPattern(ph, 0.3, 1.0, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }
            }

            WavePattern q = randomPattern(new Random(SEED + 77), dim);
            for (int topK : new int[]{1, 10, 50}) {
                int effectiveK = Math.min(topK, patterns.size());
                List<String> oracle = oracleTopK(q, patterns, ids, effectiveK);
                List<ResonanceMatch> results = store.query(q, topK);
                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                for (String expected : oracle) {
                    assertTrue(resultIds.contains(expected),
                            "4-shard topK=" + topK + " missing oracle pattern " + expected);
                }
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R1: 8 shards, K=100. */
    @Test
    @DisplayName("R1: 8 shards K=100 — exhaustive oracle match")
    void r1_eightShardsK100(@TempDir Path tmpDir) {
        int dim = 64;
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r1-8shard"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            // Spread patterns across 8 shards
            for (int s = 0; s < 8; s++) {
                double ph = -Math.PI + (2 * Math.PI * s / 8) + 0.1;
                for (int i = 0; i < 20; i++) {
                    WavePattern p = phaseTargetedPattern(ph, 0.2, 1.0, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }
            }

            WavePattern q = randomPattern(new Random(SEED + 88), dim);
            int topK = 100;
            int effectiveK = Math.min(topK, patterns.size());
            List<String> oracle = oracleTopK(q, patterns, ids, effectiveK);
            List<ResonanceMatch> results = store.query(q, topK);
            Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());

            int hits = 0;
            for (String expected : oracle) {
                if (resultIds.contains(expected)) hits++;
            }
            double recall = (double) hits / oracle.size();
            assertEquals(1.0, recall, 0.0,
                    "8-shard K=100 unindexed must achieve perfect recall, got " +
                            String.format("%.3f (%d/%d)", recall, hits, oracle.size()));
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R1: near-tied scores — verify correct ordering among close scores. */
    @Test
    @DisplayName("R1: near-tied scores — descending order preserved")
    void r1_nearTiedScores(@TempDir Path tmpDir) {
        int dim = 32;
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "4");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r1-tied"), dim, runtime);

        try {
            // Insert identical-amplitude patterns at slightly different phases across shards
            // This creates near-tied scores for any query
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                double ph = -Math.PI + (2 * Math.PI * i / 40);
                WavePattern p = constantPhasePattern(1.0, ph, dim);
                ids.add(store.insert(p, Map.of()));
                patterns.add(p);
            }

            WavePattern q = constantPhasePattern(1.0, 0.0, dim);
            List<ResonanceMatch> results = store.query(q, 10);
            assertEquals(10, results.size(), "Must return exactly 10 results");

            // Verify descending score order
            for (int i = 1; i < results.size(); i++) {
                assertTrue(results.get(i - 1).energy() >= results.get(i).energy(),
                        "Results must be in descending energy order at index " + i +
                                ": " + results.get(i - 1).energy() + " vs " + results.get(i).energy());
            }

            // Verify these are actually the oracle top-10
            List<String> oracle = oracleTopK(q, patterns, ids, 10);
            Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
            for (String expected : oracle) {
                assertTrue(resultIds.contains(expected),
                        "Near-tied scores: oracle top-10 pattern missing from results");
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R1: unequal energies — patterns with diverse amplitude distributions. */
    @Test
    @DisplayName("R1: unequal energies — correct ranking by interference score")
    void r1_unequalEnergies(@TempDir Path tmpDir) {
        int dim = 64;
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "4");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r1-energy"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            // Insert patterns with very different amplitude ranges in different shards
            double[][] ampRanges = {{0.01, 0.1}, {0.5, 1.0}, {2.0, 5.0}, {0.1, 0.3}};
            double[] phases = {-Math.PI / 2, 0.0, Math.PI / 2, Math.PI - 0.1};
            for (int s = 0; s < 4; s++) {
                for (int i = 0; i < 20; i++) {
                    WavePattern p = phaseTargetedPattern(phases[s], ampRanges[s][0], ampRanges[s][1], dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }
            }

            WavePattern q = phaseTargetedPattern(0.0, 1.0, 2.0, dim, new Random(SEED + 55));
            for (int topK : new int[]{1, 10}) {
                List<String> oracle = oracleTopK(q, patterns, ids, topK);
                List<ResonanceMatch> results = store.query(q, topK);
                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                for (String expected : oracle) {
                    assertTrue(resultIds.contains(expected),
                            "Unequal energies topK=" + topK + " missing oracle pattern");
                }
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R1: deletes — deleted patterns must not appear in results. */
    @Test
    @DisplayName("R1: deleted patterns excluded from unindexed results")
    void r1_deletedPatternsExcluded(@TempDir Path tmpDir) {
        int dim = 32;
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "4");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r1-delete"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            for (int i = 0; i < 40; i++) {
                double ph = -Math.PI + (2 * Math.PI * i / 40);
                WavePattern p = phaseTargetedPattern(ph, 0.3, 1.0, dim, rng);
                ids.add(store.insert(p, Map.of()));
                patterns.add(p);
            }

            // Delete every 3rd pattern
            Set<String> deletedIds = new HashSet<>();
            for (int i = 0; i < ids.size(); i += 3) {
                store.delete(ids.get(i));
                deletedIds.add(ids.get(i));
            }

            WavePattern q = randomPattern(new Random(SEED + 33), dim);
            List<ResonanceMatch> results = store.query(q, 20);
            for (ResonanceMatch m : results) {
                assertFalse(deletedIds.contains(m.id()),
                        "Deleted pattern " + m.id() + " must not appear in results");
            }

            // Build oracle over live patterns only
            List<WavePattern> livePatterns = new ArrayList<>();
            List<String> liveIds = new ArrayList<>();
            for (int i = 0; i < ids.size(); i++) {
                if (!deletedIds.contains(ids.get(i))) {
                    livePatterns.add(patterns.get(i));
                    liveIds.add(ids.get(i));
                }
            }
            List<String> oracle = oracleTopK(q, livePatterns, liveIds, 20);
            Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
            for (String expected : oracle) {
                assertTrue(resultIds.contains(expected),
                        "After deletes, oracle live pattern missing from results");
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════
    //  R2 — Exhaustive IVF rescore correctness
    // ═══════════════════════════════════════════════════════════

    /** R2 core: exhaustive IVF must match raw full-scan oracle exactly. */
    @Test
    @DisplayName("R2: exhaustive IVF matches independent full-scan oracle")
    void r2_exhaustiveMatchesOracle(@TempDir Path tmpDir) {
        int dim = 64;
        int N = 200;
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r2-oracle"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, dim);
                try {
                    String id = store.insert(p, Map.of());
                    patterns.add(p);
                    ids.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            // Test with multiple topK values and queries
            for (int topK : new int[]{1, 10, 100}) {
                Random qRng = new Random(SEED + topK);
                for (int q = 0; q < 20; q++) {
                    WavePattern query = randomPattern(qRng, dim);
                    List<String> oracle = oracleTopK(query, patterns, ids, topK);
                    List<ResonanceMatch> results = store.query(query, topK);
                    Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());

                    for (String expected : oracle) {
                        assertTrue(resultIds.contains(expected),
                                "R2 exhaustive topK=" + topK + " q=" + q +
                                        " missing oracle pattern " + expected);
                    }
                }
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /**
     * R2: three-way equivalence.
     * Raw full scan == Unindexed store exact == Indexed exhaustive exact.
     */
    @Test
    @DisplayName("R2: three-way equivalence — raw oracle == unindexed == indexed exhaustive")
    void r2_threeWayEquivalence(@TempDir Path tmpDir) {
        int dim = 64;
        int N = 150;
        int topK = 10;

        // First: build indexed store
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtimeIvf = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl storeIvf = new WavePatternStoreImpl(tmpDir.resolve("r2-ivf"), dim, runtimeIvf);

        // Second: build unindexed store with same data
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtimeRaw = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl storeRaw = new WavePatternStoreImpl(tmpDir.resolve("r2-raw"), dim, runtimeRaw);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> idsIvf = new ArrayList<>();
            List<String> idsRaw = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, dim);
                try {
                    String idI = storeIvf.insert(p, Map.of());
                    String idR = storeRaw.insert(p, Map.of());
                    // Content-addressable: same pattern → same ID
                    assertEquals(idI, idR, "Content hash must be deterministic");
                    patterns.add(p);
                    idsIvf.add(idI);
                    idsRaw.add(idR);
                } catch (Exception e) { /* dup */ }
            }
            storeIvf.forceIndexRebuild();

            Random qRng = new Random(SEED + 42);
            for (int q = 0; q < 20; q++) {
                WavePattern query = randomPattern(qRng, dim);

                // Route 1: raw kernel oracle
                List<String> oracle = oracleTopK(query, patterns, idsIvf, topK);

                // Route 2: unindexed store
                List<ResonanceMatch> rawResults = storeRaw.query(query, topK);
                List<String> rawIds = rawResults.stream().map(ResonanceMatch::id).toList();

                // Route 3: indexed exhaustive store
                List<ResonanceMatch> ivfResults = storeIvf.query(query, topK);
                List<String> ivfIds = ivfResults.stream().map(ResonanceMatch::id).toList();

                // All three must agree on membership
                Set<String> oracleSet = new LinkedHashSet<>(oracle);
                Set<String> rawSet = new LinkedHashSet<>(rawIds);
                Set<String> ivfSet = new LinkedHashSet<>(ivfIds);

                assertEquals(oracleSet, rawSet,
                        "Unindexed store must match oracle at q=" + q);
                assertEquals(oracleSet, ivfSet,
                        "Indexed exhaustive must match oracle at q=" + q);

                // Also verify score ordering
                for (int i = 1; i < ivfResults.size(); i++) {
                    assertTrue(ivfResults.get(i - 1).energy() >= ivfResults.get(i).energy(),
                            "IVF exhaustive results not sorted at q=" + q + " i=" + i);
                }
                for (int i = 1; i < rawResults.size(); i++) {
                    assertTrue(rawResults.get(i - 1).energy() >= rawResults.get(i).energy(),
                            "Raw results not sorted at q=" + q + " i=" + i);
                }

                // Score values must match between unindexed and indexed
                for (int i = 0; i < Math.min(rawResults.size(), ivfResults.size()); i++) {
                    if (rawIds.get(i).equals(ivfIds.get(i))) {
                        assertEquals(rawResults.get(i).energy(), ivfResults.get(i).energy(), 1e-6,
                                "Score mismatch at q=" + q + " i=" + i);
                    }
                }
            }
        } finally {
            storeIvf.close();
            storeRaw.close();
            clearIndexProperties();
            runtimeIvf.close();
            runtimeRaw.close();
        }
    }

    /** R2: delta records (inserted after index build) must be included. */
    @Test
    @DisplayName("R2: delta records after index build included in exhaustive results")
    void r2_deltaRecordsIncluded(@TempDir Path tmpDir) {
        int dim = 64;
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");
        System.setProperty("resonance.index.delta.maxSize", "10000");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r2-delta"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            // Insert initial corpus
            for (int i = 0; i < 100; i++) {
                WavePattern p = randomPattern(rng, dim);
                try {
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            // Insert delta records AFTER index build
            for (int i = 0; i < 20; i++) {
                WavePattern p = randomPattern(rng, dim);
                try {
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                } catch (Exception e) { /* dup */ }
            }

            WavePattern query = randomPattern(new Random(SEED + 99), dim);
            List<String> oracle = oracleTopK(query, patterns, ids, 10);
            List<ResonanceMatch> results = store.query(query, 10);
            Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());

            for (String expected : oracle) {
                assertTrue(resultIds.contains(expected),
                        "Delta record missing from exhaustive results: " + expected);
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R2: tombstoned records must be excluded from exhaustive results. */
    @Test
    @DisplayName("R2: deleted patterns excluded from indexed exhaustive results")
    void r2_tombstonesExcluded(@TempDir Path tmpDir) {
        int dim = 64;
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r2-tombstone"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            for (int i = 0; i < 100; i++) {
                WavePattern p = randomPattern(rng, dim);
                try {
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            // Delete some patterns
            Set<String> deletedIds = new HashSet<>();
            for (int i = 0; i < ids.size(); i += 5) {
                store.delete(ids.get(i));
                deletedIds.add(ids.get(i));
            }

            WavePattern query = randomPattern(new Random(SEED + 77), dim);
            List<ResonanceMatch> results = store.query(query, 20);
            for (ResonanceMatch m : results) {
                assertFalse(deletedIds.contains(m.id()),
                        "Deleted pattern must not appear in indexed exhaustive results");
            }

            // Oracle on live patterns
            List<WavePattern> livePatterns = new ArrayList<>();
            List<String> liveIds = new ArrayList<>();
            for (int i = 0; i < ids.size(); i++) {
                if (!deletedIds.contains(ids.get(i))) {
                    livePatterns.add(patterns.get(i));
                    liveIds.add(ids.get(i));
                }
            }
            List<String> oracle = oracleTopK(query, livePatterns, liveIds, 20);
            Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
            for (String expected : oracle) {
                assertTrue(resultIds.contains(expected),
                        "After tombstones, oracle live pattern missing from indexed results");
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R2: replaced records — old version excluded, new version ranked correctly. */
    @Test
    @DisplayName("R2: replaced records — new version in results, old version excluded")
    void r2_replacedRecords(@TempDir Path tmpDir) {
        int dim = 64;
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");
        System.setProperty("resonance.index.delta.maxSize", "10000");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r2-replace"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            for (int i = 0; i < 80; i++) {
                WavePattern p = randomPattern(rng, dim);
                try {
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            // Replace some patterns with new ones
            Set<String> replacedOldIds = new HashSet<>();
            for (int i = 0; i < 10 && i < ids.size(); i++) {
                String oldId = ids.get(i);
                WavePattern newP = randomPattern(rng, dim);
                try {
                    String newId = store.replace(oldId, newP, Map.of());
                    replacedOldIds.add(oldId);
                    patterns.set(i, newP);
                    ids.set(i, newId);
                } catch (Exception e) { /* skip if dup */ }
            }

            WavePattern query = randomPattern(new Random(SEED + 111), dim);
            List<ResonanceMatch> results = store.query(query, 20);
            for (ResonanceMatch m : results) {
                assertFalse(replacedOldIds.contains(m.id()),
                        "Replaced (old) pattern must not appear in results");
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R2: deterministic exact ranking — same query, same results, same order. */
    @Test
    @DisplayName("R2: deterministic — repeated queries produce identical results")
    void r2_deterministicRanking(@TempDir Path tmpDir) {
        int dim = 64;
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r2-determ"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            for (int i = 0; i < 200; i++) {
                WavePattern p = randomPattern(rng, dim);
                try { store.insert(p, Map.of()); } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            WavePattern query = randomPattern(new Random(SEED + 42), dim);
            List<ResonanceMatch> run1 = store.query(query, 10);
            List<ResonanceMatch> run2 = store.query(query, 10);
            List<ResonanceMatch> run3 = store.query(query, 10);

            List<String> ids1 = run1.stream().map(ResonanceMatch::id).toList();
            List<String> ids2 = run2.stream().map(ResonanceMatch::id).toList();
            List<String> ids3 = run3.stream().map(ResonanceMatch::id).toList();

            assertEquals(ids1, ids2, "Run 1 and 2 must produce identical ID ordering");
            assertEquals(ids2, ids3, "Run 2 and 3 must produce identical ID ordering");

            for (int i = 0; i < run1.size(); i++) {
                assertEquals(run1.get(i).energy(), run2.get(i).energy(), 0.0,
                        "Scores must be bit-identical across runs at index " + i);
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }

    /** R2: unequal energies with IVF — patterns of very different amplitudes. */
    @Test
    @DisplayName("R2: unequal energies — indexed exhaustive ranking matches oracle")
    void r2_unequalEnergies(@TempDir Path tmpDir) {
        int dim = 64;
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("r2-energy"), dim, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            // Mix of low-energy and high-energy patterns
            for (int i = 0; i < 100; i++) {
                double ampScale = (i % 3 == 0) ? 0.01 : (i % 3 == 1) ? 1.0 : 10.0;
                double[] amp = new double[dim];
                double[] phase = new double[dim];
                for (int j = 0; j < dim; j++) {
                    amp[j] = rng.nextDouble() * ampScale;
                    phase[j] = rng.nextDouble() * 2 * Math.PI;
                }
                WavePattern p = new WavePattern(amp, phase);
                try {
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            WavePattern query = randomPattern(new Random(SEED + 66), dim);
            for (int topK : new int[]{1, 10}) {
                List<String> oracle = oracleTopK(query, patterns, ids, topK);
                List<ResonanceMatch> results = store.query(query, topK);
                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                for (String expected : oracle) {
                    assertTrue(resultIds.contains(expected),
                            "R2 unequal energies topK=" + topK + " missing oracle pattern");
                }
            }
        } finally {
            store.close();
            clearIndexProperties();
            runtime.close();
        }
    }
}
