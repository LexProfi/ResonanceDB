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
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ExactSearchOracleTest {

    private static final long SEED = 161803L;
    private static final int DIM = 64;
    private static final int N = 300;
    private static final int TOP_K = 10;
    private static final int Q_COUNT = 40;
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

    private static List<String> oracleTopK(WavePattern query, List<WavePattern> patterns,
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

        List<String> result = new ArrayList<>();
        for (int i = 0; i < topK && i < sortedIdx.length; i++) {
            result.add(ids.get(sortedIdx[i]));
        }
        return result;
    }

    private void assertPerfectRecall(WavePatternStoreImpl store, List<WavePattern> patterns,
                                      List<String> ids, CompareOptions opts, String label) {
        Random qRng = new Random(SEED + 777);
        for (int q = 0; q < Q_COUNT; q++) {
            WavePattern query = randomPattern(qRng, DIM);
            List<String> truth = oracleTopK(query, patterns, ids, TOP_K, opts);
            List<ResonanceMatch> results = opts != null
                    ? store.query(query, TOP_K, opts)
                    : store.query(query, TOP_K);
            List<String> resultIds = results.stream()
                    .map(ResonanceMatch::id).toList();

            assertEquals(truth, resultIds,
                    label + " q=" + q + ": exact search must match oracle top-K (IDs and order)");
        }
        assertEquals(0L, store.getSegmentsSkippedCacheMiss(),
                label + ": zero segments must be skipped");
    }

    @Test
    @DisplayName("Exact search: default options — 100% recall vs oracle")
    void exactDefaultOptions(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("exact-default"), DIM, runtime);
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
                } catch (DuplicatePatternException e) { /* dup */ }
            }

            assertPerfectRecall(store, patterns, ids, null, "default");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            runtime.close();
        }
    }

    @Test
    @DisplayName("Exact search: phaseFree — 100% recall vs oracle")
    void exactPhaseFree(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("exact-phasefree"), DIM, runtime);
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
                } catch (DuplicatePatternException e) { /* dup */ }
            }

            CompareOptions phaseFree = new CompareOptions(false, true, false, false);
            assertPerfectRecall(store, patterns, ids, phaseFree, "phaseFree");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            runtime.close();
        }
    }

    @Test
    @DisplayName("Exact search: best match in maximally distant phase segment is found")
    void bestMatchInDistantPhaseSegment(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("exact-distant"), DIM, runtime);
        try {
            Random rng = new Random(SEED);
            List<WavePattern> patterns = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                double[] amp = new double[DIM];
                double[] phase = new double[DIM];
                for (int d = 0; d < DIM; d++) {
                    amp[d] = 0.5 + rng.nextDouble();
                    phase[d] = (rng.nextDouble() - 0.5) * 0.3;
                }
                WavePattern p = new WavePattern(amp, phase);
                try {
                    String id = store.insert(p, Map.of());
                    patterns.add(p);
                    ids.add(id);
                } catch (DuplicatePatternException e) { /* dup */ }
            }

            double[] bestAmp = new double[DIM];
            double[] bestPhase = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                bestAmp[d] = 1.5;
                bestPhase[d] = Math.PI - 0.01;
            }
            WavePattern bestCandidate = new WavePattern(bestAmp, bestPhase);
            String bestId = store.insert(bestCandidate, Map.of());
            patterns.add(bestCandidate);
            ids.add(bestId);

            double[] qAmp = new double[DIM];
            double[] qPhase = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                qAmp[d] = 1.5;
                qPhase[d] = Math.PI;
            }
            WavePattern query = new WavePattern(qAmp, qPhase);

            List<ResonanceMatch> results = store.query(query, 1);
            assertFalse(results.isEmpty(), "must return at least one result");
            assertEquals(bestId, results.get(0).id(),
                    "best match in distant phase segment must be found by exact scan");

            List<String> oracleBest = oracleTopK(query, patterns, ids, 1, null);
            assertEquals(oracleBest.get(0), results.get(0).id(),
                    "exact search result must match oracle");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            runtime.close();
        }
    }

    @Test
    @DisplayName("Exact search: deterministic tie-breaking by ID")
    void deterministicTieBreaking(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("exact-tiebreak"), DIM, runtime);
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
                } catch (DuplicatePatternException e) { /* dup */ }
            }

            WavePattern query = randomPattern(new Random(42), DIM);
            List<ResonanceMatch> r1 = store.query(query, TOP_K);
            List<ResonanceMatch> r2 = store.query(query, TOP_K);

            List<String> ids1 = r1.stream().map(ResonanceMatch::id).toList();
            List<String> ids2 = r2.stream().map(ResonanceMatch::id).toList();
            assertEquals(ids1, ids2, "repeated exact queries must return identical results");

            for (int i = 0; i < r1.size(); i++) {
                assertEquals(r1.get(i).energy(), r2.get(i).energy(), 0.0f,
                        "scores must be bit-identical for same query");
            }
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            runtime.close();
        }
    }
}
