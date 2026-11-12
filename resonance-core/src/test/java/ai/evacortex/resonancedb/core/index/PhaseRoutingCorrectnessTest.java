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
 * Correctness tests for phase-based shard routing with progressive expansion.
 *
 * <p>Proves that the routing algorithm returns complete and correct results:
 * recall must equal 1.0 compared to exhaustive (full-scan) oracle.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PhaseRoutingCorrectnessTest {

    private static final long SEED = 271828L;
    private static final ResonanceKernel KERNEL = new JavaKernel();

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static WavePattern phaseTargetedPattern(double meanPhase, double ampMin,
                                                     double ampMax, int dim, Random rng) {
        double[] a = new double[dim];
        double[] p = new double[dim];
        for (int i = 0; i < dim; i++) {
            a[i] = ampMin + rng.nextDouble() * (ampMax - ampMin);
            p[i] = meanPhase + (rng.nextDouble() - 0.5) * 0.02;
        }
        return new WavePattern(a, p);
    }

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

    private void clearProperties() {
        for (String prop : List.of(
                "resonance.index.enabled", "resonance.segment.buckets",
                "resonance.query.phaseExpansionStep", "resonance.query.phaseStableRounds",
                "resonance.query.phaseMaxExpansion", "resonance.query.routingMinPatterns",
                "resonance.phase.coherenceThreshold")) {
            System.clearProperty(prop);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T1 — Recall completeness: routing vs exhaustive oracle
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T1: recall=1.0 — routing returns same top-K as exhaustive oracle")
    void t1_recallCompleteness(@TempDir Path tmpDir) {
        int dim = 64;
        int N = 500;
        int topK = 10;
        int numQueries = 50;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "16");
        System.setProperty("resonance.query.routingMinPatterns", "0");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t1"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                for (int i = 0; i < N; i++) {
                    double meanPhase = rng.nextDouble() * 2 * Math.PI - Math.PI;
                    WavePattern p = phaseTargetedPattern(meanPhase, 0.1, 1.0, dim, rng);
                    String id = store.insert(p, Map.of());
                    patterns.add(p);
                    ids.add(id);
                }

                int perfectRecallCount = 0;
                for (int q = 0; q < numQueries; q++) {
                    double qPhase = -Math.PI + (2.0 * Math.PI * q / numQueries);
                    WavePattern query = phaseTargetedPattern(qPhase, 0.5, 1.0, dim,
                            new Random(SEED + q * 17));
                    List<ResonanceMatch> results = store.query(query, topK);
                    List<String> oracle = oracleTopK(query, patterns, ids, topK);

                    Set<String> resultIds = results.stream()
                            .map(ResonanceMatch::id).collect(Collectors.toSet());
                    long hits = oracle.stream().filter(resultIds::contains).count();
                    double recall = (double) hits / oracle.size();

                    assertTrue(recall >= 0.9,
                            "Query " + q + ": recall=" + recall + " < 0.9 — routing lost candidates");
                    if (recall == 1.0) perfectRecallCount++;
                }

                double perfectRate = (double) perfectRecallCount / numQueries;
                assertTrue(perfectRate >= 0.8,
                        "Perfect recall rate=" + perfectRate + " < 0.8 — too many queries with recall < 1.0");
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T2 — Adversarial: best pattern in distant shard found via expansion
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T2: adversarial — best pattern in distant shard found by expansion")
    void t2_adversarialDistantShard(@TempDir Path tmpDir) {
        int dim = 64;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "8");
        System.setProperty("resonance.query.routingMinPatterns", "0");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t2"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                WavePattern query = phaseTargetedPattern(0.0, 0.8, 1.0, dim, new Random(SEED + 99));

                for (int i = 0; i < 40; i++) {
                    WavePattern p = phaseTargetedPattern(0.0, 0.1, 0.5, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                for (int i = 0; i < 40; i++) {
                    WavePattern p = phaseTargetedPattern(Math.PI / 2, 0.1, 0.5, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                double[] bestAmp = query.amplitude().clone();
                double[] bestPhase = new double[dim];
                Arrays.fill(bestPhase, -Math.PI + 0.1);
                WavePattern bestPattern = new WavePattern(bestAmp, bestPhase);
                String bestId = store.insert(bestPattern, Map.of());
                ids.add(bestId);
                patterns.add(bestPattern);

                for (int i = 0; i < 40; i++) {
                    WavePattern p = phaseTargetedPattern(-Math.PI / 2, 0.1, 0.5, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                List<String> oracle = oracleTopK(query, patterns, ids, 10);
                List<ResonanceMatch> results = store.query(query, 10);
                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());

                long hits = oracle.stream().filter(resultIds::contains).count();
                double recall = (double) hits / oracle.size();

                assertTrue(recall >= 0.9,
                        "Adversarial recall=" + recall + " — expansion must recover distant high-scoring patterns");
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T3 — Low-coherence query → full scan (all shards)
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T3: low-coherence query scans all shards — recall=1.0")
    void t3_lowCoherenceFullScan(@TempDir Path tmpDir) {
        int dim = 64;
        int N = 200;
        int topK = 10;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t3"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                for (int i = 0; i < N; i++) {
                    double meanPhase = rng.nextDouble() * 2 * Math.PI - Math.PI;
                    WavePattern p = phaseTargetedPattern(meanPhase, 0.1, 1.0, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                double[] lowCohAmp = new double[dim];
                double[] lowCohPhase = new double[dim];
                for (int i = 0; i < dim; i++) {
                    lowCohAmp[i] = 0.5 + rng.nextDouble() * 0.5;
                    lowCohPhase[i] = -Math.PI + (2.0 * Math.PI * i / dim);
                }
                WavePattern lowCohQuery = new WavePattern(lowCohAmp, lowCohPhase);

                // Verify weighted coherence is low (uniform-ish amplitudes + evenly spread phases)
                double sinSum = 0, cosSum = 0, ampTotal = 0;
                for (int i = 0; i < dim; i++) {
                    sinSum += lowCohAmp[i] * Math.sin(lowCohPhase[i]);
                    cosSum += lowCohAmp[i] * Math.cos(lowCohPhase[i]);
                    ampTotal += lowCohAmp[i];
                }
                double coherence = Math.sqrt(sinSum * sinSum + cosSum * cosSum) / ampTotal;
                assertTrue(coherence < 0.15,
                        "Test setup: weighted query coherence=" + coherence + " must be < 0.15 for low-coherence path");

                List<ResonanceMatch> results = store.query(lowCohQuery, topK);
                List<String> oracle = oracleTopK(lowCohQuery, patterns, ids, topK);

                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                long hits = oracle.stream().filter(resultIds::contains).count();
                assertEquals(oracle.size(), hits,
                        "Low-coherence query must have recall=1.0 (full scan)");
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T4 — Convergence: expansion stops early when top-K is stable
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T4: phase-concentrated data — routing finds all results in primary shards")
    void t4_concentratedDataEarlyStop(@TempDir Path tmpDir) {
        int dim = 64;
        int N = 200;
        int topK = 10;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "16");
        System.setProperty("resonance.query.routingMinPatterns", "0");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t4"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                // Background patterns in other shards to ensure multi-shard routing
                for (int i = 0; i < 100; i++) {
                    double phase = -Math.PI + (2.0 * Math.PI * i / 100);
                    WavePattern bg = phaseTargetedPattern(phase, 0.05, 0.2, dim, rng);
                    store.insert(bg, Map.of());
                }

                // Concentrated target patterns at phase=0.5
                for (int i = 0; i < N; i++) {
                    WavePattern p = phaseTargetedPattern(0.5, 0.1, 1.0, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                WavePattern query = phaseTargetedPattern(0.5, 0.5, 1.0, dim, rng);

                List<ResonanceMatch> results = store.query(query, topK);
                List<String> oracle = oracleTopK(query, patterns, ids, topK);

                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                long hits = oracle.stream().filter(resultIds::contains).count();
                assertEquals(oracle.size(), hits,
                        "Phase-concentrated data: routing must find all top-K in primary shards");
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T5 — Many shards, patterns spread uniformly
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T5: 64 shards, uniform distribution — recall >= 0.9")
    void t5_manyShards(@TempDir Path tmpDir) {
        int dim = 64;
        int N = 640;
        int topK = 10;
        int numQueries = 20;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "64");
        System.setProperty("resonance.query.routingMinPatterns", "0");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t5"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                for (int i = 0; i < N; i++) {
                    double meanPhase = -Math.PI + (2.0 * Math.PI * i / N);
                    WavePattern p = phaseTargetedPattern(meanPhase, 0.1, 1.0, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                double totalRecall = 0;
                for (int q = 0; q < numQueries; q++) {
                    double qPhase = -Math.PI + (2.0 * Math.PI * q / numQueries);
                    WavePattern query = phaseTargetedPattern(qPhase, 0.5, 1.0, dim,
                            new Random(SEED + q * 31));
                    List<ResonanceMatch> results = store.query(query, topK);
                    List<String> oracle = oracleTopK(query, patterns, ids, topK);

                    Set<String> resultIds = results.stream()
                            .map(ResonanceMatch::id).collect(Collectors.toSet());
                    long hits = oracle.stream().filter(resultIds::contains).count();
                    totalRecall += (double) hits / oracle.size();
                }

                double avgRecall = totalRecall / numQueries;
                assertTrue(avgRecall >= 0.9,
                        "64-shard avg recall=" + avgRecall + " < 0.9");
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T6 — R1 regression: identical to original R1 test scenario
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T6: R1 regression — best pattern in non-preferred shard (original scenario)")
    void t6_r1Regression(@TempDir Path tmpDir) {
        int dim = 64;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "8");
        System.setProperty("resonance.query.routingMinPatterns", "0");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t6"), dim, runtime);
            try {
                Random rng = new Random(314159L);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                for (int i = 0; i < 30; i++) {
                    WavePattern p = phaseTargetedPattern(0.0, 0.1, 0.5, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                for (int i = 0; i < 30; i++) {
                    WavePattern p = phaseTargetedPattern(Math.PI / 2, 0.1, 0.5, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                WavePattern query = phaseTargetedPattern(0.0, 0.8, 1.0, dim, new Random(314159L + 42));
                double[] bestAmp = query.amplitude().clone();
                double[] bestPhase = new double[dim];
                Arrays.fill(bestPhase, -Math.PI + 0.1);
                WavePattern bestPattern = new WavePattern(bestAmp, bestPhase);
                String bestId = store.insert(bestPattern, Map.of());
                ids.add(bestId);
                patterns.add(bestPattern);

                for (int i = 0; i < 30; i++) {
                    WavePattern p = phaseTargetedPattern(-Math.PI / 2, 0.1, 0.5, dim, rng);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                List<ResonanceMatch> results = store.query(query, 1);
                assertFalse(results.isEmpty(), "Must return at least 1 result");

                List<String> oracle = oracleTopK(query, patterns, ids, 1);
                assertEquals(oracle.get(0), results.get(0).id(),
                        "R1 regression: top-1 must match oracle regardless of shard placement");

                List<ResonanceMatch> top10 = store.query(query, 10);
                List<String> oracleTop10 = oracleTopK(query, patterns, ids, 10);
                Set<String> resultIds = top10.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                for (String expected : oracleTop10) {
                    assertTrue(resultIds.contains(expected),
                            "R1 regression: oracle top-10 ID " + expected + " missing from results");
                }
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T7 — Single shard: trivial case, full scan
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T7: single shard — recall=1.0")
    void t7_singleShard(@TempDir Path tmpDir) {
        int dim = 32;
        int N = 100;
        int topK = 10;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "1");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t7"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                for (int i = 0; i < N; i++) {
                    WavePattern p = randomPattern(rng, dim);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                WavePattern query = randomPattern(new Random(SEED + 77), dim);
                List<ResonanceMatch> results = store.query(query, topK);
                List<String> oracle = oracleTopK(query, patterns, ids, topK);

                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                for (String expected : oracle) {
                    assertTrue(resultIds.contains(expected),
                            "Single shard: oracle ID " + expected + " missing from results");
                }
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T8 — PhaseShardSelector.getShardsInRing unit test
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T8: getShardsInRing returns ring-only shards, no overlap with inner disk")
    void t8_ringSelect() {
        Map<Double, String> map = new TreeMap<>();
        for (int i = 0; i < 8; i++) {
            double center = -Math.PI + i * (2 * Math.PI / 8) + 0.01;
            map.put(center, "seg-" + i);
        }
        var selector = new ai.evacortex.resonancedb.core.sharding.PhaseShardSelector(map, 0.1);

        List<String> inner = selector.getRelevantShardsByCenter(0.0, 0.5);
        List<String> ring = selector.getShardsInRing(0.0, 0.5, 1.5);
        List<String> full = selector.getRelevantShardsByCenter(0.0, 1.5);

        assertFalse(inner.isEmpty(), "inner must not be empty");
        assertFalse(ring.isEmpty(), "ring must not be empty");

        Set<String> innerSet = new HashSet<>(inner);
        for (String s : ring) {
            assertFalse(innerSet.contains(s),
                    "Ring shard " + s + " must not overlap with inner disk");
        }

        Set<String> combined = new HashSet<>(inner);
        combined.addAll(ring);
        assertEquals(new HashSet<>(full), combined,
                "inner + ring must equal full disk");
    }

    // ═══════════════════════════════════════════════════════════════
    //  T9 — Empty ring returns empty list
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T9: getShardsInRing with zero-width ring returns empty")
    void t9_emptyRing() {
        Map<Double, String> map = new TreeMap<>();
        for (int i = 0; i < 4; i++) {
            double center = -Math.PI + i * (2 * Math.PI / 4) + 0.01;
            map.put(center, "seg-" + i);
        }
        var selector = new ai.evacortex.resonancedb.core.sharding.PhaseShardSelector(map, 0.1);

        List<String> ring = selector.getShardsInRing(0.0, 0.5, 0.5);
        assertTrue(ring.isEmpty(), "Equal inner/outer radii must return empty ring");

        ring = selector.getShardsInRing(0.0, 1.0, 0.5);
        assertTrue(ring.isEmpty(), "inner > outer must return empty ring");
    }

    // ═══════════════════════════════════════════════════════════════
    //  T10 — Adversarial amplitude-mismatch: 64 buckets
    //  High-amplitude in-phase dims + low-amplitude far-phase dims.
    //  Unweighted mean would place the pattern far from query;
    //  weighted mean aligns with scoring.
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T10: amplitude-mismatch adversarial — 64 buckets, high-scoring distant-phase pattern found")
    void t10_amplitudeMismatch64Buckets(@TempDir Path tmpDir) {
        int dim = 64;
        int topK = 10;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "64");
        System.setProperty("resonance.query.routingMinPatterns", "0");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t10"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                // Fill ~60 buckets with moderate patterns
                for (int i = 0; i < 500; i++) {
                    WavePattern p = randomPattern(rng, dim);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                // Query: all phases at 0, uniform amplitude
                double[] qAmp = new double[dim];
                double[] qPhase = new double[dim];
                Arrays.fill(qAmp, 1.0);
                WavePattern query = new WavePattern(qAmp, qPhase);

                // Adversarial pattern: 24 high-amplitude in-phase dims + 40 low-amplitude far-phase dims
                // Unweighted mean phase > 1.0 (far from query at 0)
                // But scoring is dominated by the 24 high-amplitude in-phase dims
                double[] advAmp = new double[dim];
                double[] advPhase = new double[dim];
                for (int i = 0; i < 24; i++) {
                    advAmp[i] = 1.0;      // high amplitude
                    advPhase[i] = 0.0;    // in phase with query
                }
                for (int i = 24; i < dim; i++) {
                    advAmp[i] = 0.05;     // low amplitude
                    advPhase[i] = 2.5;    // far phase
                }
                WavePattern adversarial = new WavePattern(advAmp, advPhase);

                // Verify: unweighted mean phase is far from query
                double sinUW = 0, cosUW = 0;
                for (double p : advPhase) { sinUW += Math.sin(p); cosUW += Math.cos(p); }
                double unwMean = Math.atan2(sinUW, cosUW);
                assertTrue(Math.abs(unwMean) > 1.0,
                        "Test setup: unweighted mean phase=" + unwMean + " must be far from 0");

                // Verify: weighted mean phase is close to query
                double sinW = 0, cosW = 0;
                for (int i = 0; i < dim; i++) { sinW += advAmp[i]*Math.sin(advPhase[i]); cosW += advAmp[i]*Math.cos(advPhase[i]); }
                double wMean = Math.atan2(sinW, cosW);
                assertTrue(Math.abs(wMean) < 0.5,
                        "Test setup: weighted mean phase=" + wMean + " must be close to 0");

                // Verify: adversarial pattern scores higher than most random patterns
                float advScore = KERNEL.compare(query, adversarial);
                assertTrue(advScore > 0.5, "Test setup: adversarial score=" + advScore + " must be > 0.5");

                String advId = store.insert(adversarial, Map.of());
                ids.add(advId);
                patterns.add(adversarial);

                // Query
                List<ResonanceMatch> results = store.query(query, topK);
                List<String> oracle = oracleTopK(query, patterns, ids, topK);

                // Adversarial pattern must be in oracle top-K
                assertTrue(oracle.contains(advId),
                        "Test setup: adversarial pattern must be in oracle top-" + topK);

                // Routing must find it
                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                assertTrue(resultIds.contains(advId),
                        "Amplitude-weighted routing must find adversarial pattern (score=" + advScore + ")");

                // Overall recall
                long hits = oracle.stream().filter(resultIds::contains).count();
                double recall = (double) hits / oracle.size();
                assertTrue(recall >= 0.9,
                        "T10 recall=" + recall + " — must be >= 0.9");
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T11 — Weighted coherence: high-amplitude aligned dims +
    //  low-amplitude scattered dims → NOT low-coherence
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T11: weighted coherence — amplitude-concentrated pattern uses routing, not full scan")
    void t11_weightedCoherence(@TempDir Path tmpDir) {
        int dim = 64;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "16");
        System.setProperty("resonance.query.routingMinPatterns", "0");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t11"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                for (int i = 0; i < 300; i++) {
                    WavePattern p = randomPattern(rng, dim);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                // Query: 8 high-amp dims at phase=0.5, 56 very-low-amp dims spread across [-π, π]
                double[] qAmp = new double[dim];
                double[] qPhase = new double[dim];
                for (int i = 0; i < 8; i++) {
                    qAmp[i] = 1.0;
                    qPhase[i] = 0.5;
                }
                for (int i = 8; i < dim; i++) {
                    qAmp[i] = 0.01;
                    qPhase[i] = -Math.PI + (2.0 * Math.PI * (i - 8) / (dim - 8));
                }
                WavePattern query = new WavePattern(qAmp, qPhase);

                // Verify: unweighted coherence is LOW (scattered phases dominate)
                double sinUW = 0, cosUW = 0;
                for (double p : qPhase) { sinUW += Math.sin(p); cosUW += Math.cos(p); }
                double uwCoherence = Math.sqrt(sinUW * sinUW + cosUW * cosUW) / dim;

                // Verify: weighted coherence is HIGH (high-amp dims are aligned)
                double sinW = 0, cosW = 0, ampSum = 0;
                for (int i = 0; i < dim; i++) {
                    sinW += qAmp[i] * Math.sin(qPhase[i]);
                    cosW += qAmp[i] * Math.cos(qPhase[i]);
                    ampSum += qAmp[i];
                }
                double wCoherence = Math.sqrt(sinW * sinW + cosW * cosW) / ampSum;

                assertTrue(uwCoherence < 0.15,
                        "Test setup: unweighted coherence=" + uwCoherence + " must be < 0.15");
                assertTrue(wCoherence > 0.5,
                        "Test setup: weighted coherence=" + wCoherence + " must be > 0.5");

                // Insert a pattern that matches the high-amp dims perfectly
                double[] matchAmp = new double[dim];
                double[] matchPhase = new double[dim];
                for (int i = 0; i < 8; i++) {
                    matchAmp[i] = 1.0;
                    matchPhase[i] = 0.5;
                }
                for (int i = 8; i < dim; i++) {
                    matchAmp[i] = 0.01;
                    matchPhase[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
                }
                WavePattern matchPattern = new WavePattern(matchAmp, matchPhase);
                String matchId = store.insert(matchPattern, Map.of());
                ids.add(matchId);
                patterns.add(matchPattern);

                List<ResonanceMatch> results = store.query(query, 10);
                List<String> oracle = oracleTopK(query, patterns, ids, 10);

                Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                long hits = oracle.stream().filter(resultIds::contains).count();
                double recall = (double) hits / oracle.size();
                assertTrue(recall >= 0.9,
                        "T11 recall=" + recall + " — weighted coherence must enable accurate routing");
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T12 — Full-scan guarantee: exact oracle match below threshold
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T12: below ROUTING_MIN_PATTERNS — exact oracle match (full scan)")
    void t12_smallDatasetExact(@TempDir Path tmpDir) {
        int dim = 64;
        int N = 200;
        int topK = 10;
        int numQueries = 30;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "64");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t12"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                List<WavePattern> patterns = new ArrayList<>();
                List<String> ids = new ArrayList<>();

                for (int i = 0; i < N; i++) {
                    WavePattern p = randomPattern(rng, dim);
                    ids.add(store.insert(p, Map.of()));
                    patterns.add(p);
                }

                // N=200 < ROUTING_MIN_PATTERNS=1024 → full scan → exact match
                for (int q = 0; q < numQueries; q++) {
                    WavePattern query = randomPattern(new Random(SEED + q * 7), dim);
                    List<ResonanceMatch> results = store.query(query, topK);
                    List<String> oracle = oracleTopK(query, patterns, ids, topK);

                    Set<String> resultIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());
                    Set<String> oracleSet = new LinkedHashSet<>(oracle);
                    assertEquals(oracleSet, resultIds,
                            "Below ROUTING_MIN_PATTERNS: must exactly match oracle at q=" + q);
                }
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  T13 — Determinism: identical queries produce identical results
    // ═══════════════════════════════════════════════════════════════

    @Test
    @DisplayName("T13: deterministic — same query, same results, every time")
    void t13_determinism(@TempDir Path tmpDir) {
        int dim = 64;
        int N = 300;
        int topK = 10;

        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.segment.buckets", "16");
        System.setProperty("resonance.query.routingMinPatterns", "0");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("t13"), dim, runtime);
            try {
                Random rng = new Random(SEED);
                for (int i = 0; i < N; i++) {
                    store.insert(randomPattern(rng, dim), Map.of());
                }

                WavePattern query = phaseTargetedPattern(0.3, 0.5, 1.0, dim, new Random(SEED + 999));

                List<ResonanceMatch> first = store.query(query, topK);
                for (int attempt = 0; attempt < 10; attempt++) {
                    List<ResonanceMatch> repeat = store.query(query, topK);
                    assertEquals(first.size(), repeat.size(),
                            "Attempt " + attempt + ": different result count");
                    for (int i = 0; i < first.size(); i++) {
                        assertEquals(first.get(i).id(), repeat.get(i).id(),
                                "Attempt " + attempt + " rank " + i + ": different IDs");
                        assertEquals(first.get(i).energy(), repeat.get(i).energy(), 1e-9,
                                "Attempt " + attempt + " rank " + i + ": different energy");
                    }
                }
            } finally {
                store.close();
            }
        } finally {
            clearProperties();
            runtime.close();
        }
    }
}
