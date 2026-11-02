/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.PhaseWeights;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive oracle tests for IVF retrieval correctness (Issue C).
 *
 * <p>For each query, computes ground-truth top-K via exhaustive kernel scoring
 * and compares with indexed retrieval. Tests cover default, ignorePhase,
 * weighted, and phase-free options.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IvfExhaustiveOracleTest {

    private static final long SEED = 271828L;
    private static final int DIM = 64;
    private static final int N = 200;
    private static final int TOP_K = 10;
    private static final int Q_COUNT = 30;
    private static final ResonanceKernel KERNEL = new JavaKernel();

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static Set<String> oracleTopK(WavePattern query, List<WavePattern> patterns,
                                           List<String> ids, int topK, CompareOptions opts) {
        float[] scores = new float[patterns.size()];
        for (int i = 0; i < patterns.size(); i++) {
            scores[i] = opts != null
                    ? KERNEL.compare(query, patterns.get(i), opts)
                    : KERNEL.compare(query, patterns.get(i));
        }
        Integer[] sortedIdx = new Integer[patterns.size()];
        for (int i = 0; i < sortedIdx.length; i++) sortedIdx[i] = i;
        Arrays.sort(sortedIdx, (a, b) -> {
            int c = Float.compare(scores[b], scores[a]);
            return c != 0 ? c : ids.get(a).compareTo(ids.get(b));
        });

        Set<String> result = new LinkedHashSet<>();
        for (int i = 0; i < topK && i < sortedIdx.length; i++) {
            result.add(ids.get(sortedIdx[i]));
        }
        return result;
    }

    private double measureRecall(WavePatternStoreImpl store, List<WavePattern> patterns,
                                  List<String> ids, CompareOptions opts) {
        Random qRng = new Random(SEED + 999);
        int totalHits = 0, totalExpected = 0;
        for (int q = 0; q < Q_COUNT; q++) {
            WavePattern query = randomPattern(qRng, DIM);
            Set<String> truth = oracleTopK(query, patterns, ids, TOP_K, opts);
            List<ResonanceMatch> results = opts != null
                    ? store.query(query, TOP_K, opts)
                    : store.query(query, TOP_K);
            Set<String> resultIds = results.stream()
                    .map(ResonanceMatch::id).collect(Collectors.toSet());
            for (String id : truth) {
                if (resultIds.contains(id)) totalHits++;
            }
            totalExpected += truth.size();
        }
        return totalExpected > 0 ? (double) totalHits / totalExpected : 1.0;
    }

    @Test
    @DisplayName("Exact mode: default options — IVF matches exhaustive oracle")
    void exactMode_defaultOptions(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("default"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    patterns.add(p);
                    ids.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            double recall = measureRecall(store, patterns, ids, null);
            System.out.printf("Default exact recall@%d: %.3f%n", TOP_K, recall);
            assertTrue(recall >= 0.99,
                    "Exact mode default must have >= 0.99 recall, got " + String.format("%.3f", recall));
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.exactEquivalence");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @DisplayName("Exact mode: ignorePhase=true — IVF matches exhaustive phase-free oracle")
    void exactMode_ignorePhase(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("phasefree"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    patterns.add(p);
                    ids.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            CompareOptions opts = new CompareOptions(false, true, false, false);
            double recall = measureRecall(store, patterns, ids, opts);
            System.out.printf("IgnorePhase exact recall@%d: %.3f%n", TOP_K, recall);
            assertTrue(recall >= 0.99,
                    "Exact mode ignorePhase must have >= 0.99 recall, got " + String.format("%.3f", recall));
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.exactEquivalence");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @DisplayName("Exact mode: PhaseWeights — IVF matches exhaustive weighted oracle")
    void exactMode_phaseWeights(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("weighted"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    patterns.add(p);
                    ids.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            double[] w = new double[DIM];
            Random wRng = new Random(42);
            for (int i = 0; i < DIM; i++) w[i] = wRng.nextDouble();
            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(w));

            double recall = measureRecall(store, patterns, ids, opts);
            System.out.printf("PhaseWeights exact recall@%d: %.3f%n", TOP_K, recall);
            assertTrue(recall >= 0.99,
                    "Exact mode PhaseWeights must have >= 0.99 recall, got " + String.format("%.3f", recall));
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.exactEquivalence");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @DisplayName("Exact mode: queryDetailed matches query for ignorePhase")
    void exactMode_queryDetailedMatchesQuery_ignorePhase(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("detailed"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try { store.insert(p, Map.of()); } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            CompareOptions opts = new CompareOptions(false, true, false, false);
            Random qRng = new Random(SEED + 999);
            for (int q = 0; q < 10; q++) {
                WavePattern query = randomPattern(qRng, DIM);
                List<ResonanceMatch> basic = store.query(query, TOP_K, opts);
                List<ResonanceMatchDetailed> detailed = store.queryDetailed(query, TOP_K, opts);

                List<String> basicIds = basic.stream().map(ResonanceMatch::id).toList();
                List<String> detailedIds = detailed.stream().map(ResonanceMatchDetailed::id).toList();
                assertEquals(basicIds, detailedIds,
                        "query and queryDetailed must return same IDs in same order (q=" + q + ")");

                for (int i = 0; i < basic.size(); i++) {
                    assertEquals(basic.get(i).energy(), detailed.get(i).energy(), 1e-6,
                            "Energy mismatch at q=" + q + " i=" + i);
                    assertEquals(0.0, detailed.get(i).phaseDelta(), 0.0,
                            "phaseDelta must be 0.0 for ignorePhase (q=" + q + " i=" + i + ")");
                }
            }
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.exactEquivalence");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @DisplayName("Deterministic tie-breaking: same query → same result ordering")
    void deterministicTieBreaking(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "4");
        System.setProperty("resonance.index.l1.nprobe", "4");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("determ"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            for (int i = 0; i < 100; i++) {
                try { store.insert(randomPattern(rng, DIM), Map.of()); }
                catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            Random qRng = new Random(SEED + 111);
            for (int q = 0; q < 10; q++) {
                WavePattern query = randomPattern(qRng, DIM);
                List<ResonanceMatch> r1 = store.query(query, TOP_K);
                List<ResonanceMatch> r2 = store.query(query, TOP_K);
                List<String> ids1 = r1.stream().map(ResonanceMatch::id).toList();
                List<String> ids2 = r2.stream().map(ResonanceMatch::id).toList();
                assertEquals(ids1, ids2, "Deterministic tie-breaking failed at q=" + q);
            }
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.exactEquivalence");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }
}
