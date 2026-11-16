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

class CachePressureExactTest {

    private static final long SEED = 271828L;
    private static final int DIM = 64;
    private static final int N = 600;
    private static final int TOP_K = 10;
    private static final int Q_COUNT = 20;
    private static final long CACHE_BUDGET = 1L << 20;
    private static final ResonanceKernel KERNEL = new JavaKernel();

    private final String[] MANAGED_PROPS = {
            "resonance.index.enabled",
            "resonance.cache.maxBytes",
            "resonance.segment.maxBytes",
            "resonance.delta.sealThreshold",
            "resonance.delta.sealIntervalSeconds"
    };
    private final Map<String, String> savedProps = new HashMap<>();

    @BeforeEach
    void saveProps() {
        for (String key : MANAGED_PROPS) {
            savedProps.put(key, System.getProperty(key));
        }
    }

    @AfterEach
    void restoreProps() {
        for (String key : MANAGED_PROPS) {
            String original = savedProps.get(key);
            if (original == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, original);
            }
        }
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
        for (int i = 0; i < topK && i < sortedIdx.length; i++) {
            result.add(ids.get(sortedIdx[i]));
        }
        return result;
    }

    @Test
    @DisplayName("Exact search under cache pressure: zero skipped segments, oracle match")
    void exactUnderCachePressure(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.cache.maxBytes", String.valueOf(CACHE_BUDGET));
        System.setProperty("resonance.segment.maxBytes", String.valueOf(32 * 1024));
        System.setProperty("resonance.delta.sealThreshold", "50");
        System.setProperty("resonance.delta.sealIntervalSeconds", "9999");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("cache-pressure"), DIM, runtime);
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
                if ((i + 1) % 50 == 0) {
                    store.sealDelta();
                }
            }
            store.sealDelta();

            assertTrue(patterns.size() > 100, "must have enough patterns inserted");

            Random qRng = new Random(SEED + 999);
            for (int q = 0; q < Q_COUNT; q++) {
                WavePattern query = randomPattern(qRng, DIM);
                List<String> truth = oracleTopK(query, patterns, ids, TOP_K);
                List<ResonanceMatch> results = store.query(query, TOP_K);
                List<String> resultIds = results.stream()
                        .map(ResonanceMatch::id).toList();

                assertEquals(truth, resultIds,
                        "q=" + q + ": exact search under cache pressure must match oracle");
            }

            assertEquals(0L, store.getSegmentsSkippedCacheMiss(),
                    "zero segments must be skipped even under cache pressure");
        } finally {
            store.close();
            runtime.close();
        }
    }

    @Test
    @DisplayName("Self-match recall under cache pressure: inserted pattern is always top-1")
    void selfMatchRecallUnderPressure(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.cache.maxBytes", String.valueOf(CACHE_BUDGET));
        System.setProperty("resonance.segment.maxBytes", String.valueOf(32 * 1024));
        System.setProperty("resonance.delta.sealThreshold", "50");
        System.setProperty("resonance.delta.sealIntervalSeconds", "9999");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("cache-selfmatch"), DIM, runtime);
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
                if ((i + 1) % 50 == 0) {
                    store.sealDelta();
                }
            }
            store.sealDelta();

            Random sampleRng = new Random(SEED + 13);
            int sampleSize = Math.min(30, patterns.size());
            for (int s = 0; s < sampleSize; s++) {
                int idx = sampleRng.nextInt(patterns.size());
                WavePattern p = patterns.get(idx);
                String expectedId = ids.get(idx);

                List<ResonanceMatch> results = store.query(p, 1);
                assertFalse(results.isEmpty(), "must return at least one result for self-match");
                assertEquals(expectedId, results.get(0).id(),
                        "self-match must return the pattern itself as top-1");
                assertEquals(1.0f, results.get(0).energy(), 1e-4f,
                        "self-match score must be ~1.0");
            }

            assertEquals(0L, store.getSegmentsSkippedCacheMiss(),
                    "zero segments must be skipped during self-match queries");
        } finally {
            store.close();
            runtime.close();
        }
    }
}
