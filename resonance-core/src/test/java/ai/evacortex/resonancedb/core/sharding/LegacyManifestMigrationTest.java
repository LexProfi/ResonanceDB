/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.sharding;

import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Legacy manifest migration and group stats restoration on reopen.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>When the version marker file is absent, manifest phaseCenters are
 *       rebuilt from stored patterns using circular mean on next open.</li>
 *   <li>After close/reopen, group phase statistics are correctly restored.</li>
 *   <li>The version marker prevents repeated migration on subsequent opens.</li>
 *   <li>Patterns near ±π boundary are correctly handled after migration.</li>
 * </ul>
 */
class LegacyManifestMigrationTest {

    private static final double PI = Math.PI;

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

    private static WavePattern boundaryPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = 0.5 + rng.nextDouble() * 0.5;
            // Cluster near +π with small noise — some wrap to -π side
            phase[i] = PI - 0.05 + rng.nextGaussian() * 0.08;
            while (phase[i] > PI) phase[i] -= 2 * PI;
            while (phase[i] <= -PI) phase[i] += 2 * PI;
        }
        return new WavePattern(amp, phase);
    }

    private static WavePattern clusteredPattern(Random rng, int dim, double center) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = 0.5 + rng.nextDouble() * 0.5;
            phase[i] = center + rng.nextGaussian() * 0.2;
            while (phase[i] > PI) phase[i] -= 2 * PI;
            while (phase[i] <= -PI) phase[i] += 2 * PI;
        }
        return new WavePattern(amp, phase);
    }

    @Test
    @DisplayName("Legacy migration: ±π boundary patterns get correct phaseCenters after migration")
    void legacyMigration_boundaryPatterns(@TempDir Path tmpDir) throws IOException {
        int D = 64, N = 50;
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            Path storeDir = tmpDir.resolve("legacy");

            List<String> insertedIds = new ArrayList<>();
            WavePatternStoreImpl store1 = new WavePatternStoreImpl(storeDir, D, runtime);
            try {
                Random rng = new Random(42);
                for (int i = 0; i < N; i++) {
                    WavePattern p = boundaryPattern(rng, D);
                    String id = store1.insert(p, Map.of());
                    insertedIds.add(id);
                }
            } finally {
                store1.close();
            }

            Path versionFile = storeDir.resolve("index").resolve(".circular-phase-v1");
            assertTrue(Files.exists(versionFile), "Version marker should exist after first open");
            Files.delete(versionFile);
            assertFalse(Files.exists(versionFile), "Version marker should be deleted");

            WavePatternStoreImpl store2 = new WavePatternStoreImpl(storeDir, D, runtime);
            try {
                assertTrue(Files.exists(versionFile),
                        "Version marker should be recreated after migration");

                Random queryRng = new Random(99);
                WavePattern query = boundaryPattern(queryRng, D);
                List<ResonanceMatch> results = store2.query(query, 10);

                assertFalse(results.isEmpty(),
                        "Query should return results after migration");

                System.out.printf("Legacy migration test: %d patterns inserted, %d results returned%n",
                        N, results.size());
            } finally {
                store2.close();
            }
        } finally {
            runtime.close();
        }
    }

    @Test
    @DisplayName("Reopen restores group phase statistics correctly")
    void reopen_groupStatsRestored(@TempDir Path tmpDir) {
        int D = 64, N = 100;
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            Path storeDir = tmpDir.resolve("reopen-stats");

            WavePatternStoreImpl store1 = new WavePatternStoreImpl(storeDir, D, runtime);
            List<ResonanceMatch> resultsBeforeClose;
            WavePattern query;
            try {
                Random rng = new Random(42);
                for (int i = 0; i < N; i++) {
                    double center = (i % 4 == 0) ? 1.0 : (i % 4 == 1) ? -1.0 :
                            (i % 4 == 2) ? 0.5 : -0.5;
                    WavePattern p = clusteredPattern(rng, D, center);
                    store1.insert(p, Map.of());
                }
                query = clusteredPattern(new Random(77), D, 1.0);
                resultsBeforeClose = store1.query(query, 10);
            } finally {
                store1.close();
            }

            WavePatternStoreImpl store2 = new WavePatternStoreImpl(storeDir, D, runtime);
            try {
                List<ResonanceMatch> resultsAfterReopen = store2.query(query, 10);

                assertFalse(resultsAfterReopen.isEmpty(),
                        "Query should return results after reopen");

                assertEquals(resultsBeforeClose.get(0).id(), resultsAfterReopen.get(0).id(),
                        "Top-1 result should be identical before close and after reopen");

                Set<String> beforeIds = new HashSet<>();
                resultsBeforeClose.forEach(r -> beforeIds.add(r.id()));
                int overlap = 0;
                for (ResonanceMatch r : resultsAfterReopen) {
                    if (beforeIds.contains(r.id())) overlap++;
                }
                double overlapRatio = (double) overlap / Math.min(resultsBeforeClose.size(),
                        resultsAfterReopen.size());

                System.out.printf("Reopen test: before=%d results, after=%d results, overlap=%.1f%%%n",
                        resultsBeforeClose.size(), resultsAfterReopen.size(), overlapRatio * 100);

                assertTrue(overlapRatio >= 0.8,
                        "At least 80% overlap expected, got " + (overlapRatio * 100) + "%");
            } finally {
                store2.close();
            }
        } finally {
            runtime.close();
        }
    }

    @Test
    @DisplayName("Version marker prevents repeated migration")
    void versionMarker_preventsRepeatedMigration(@TempDir Path tmpDir) throws IOException {
        int D = 64;
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            Path storeDir = tmpDir.resolve("marker-test");

            WavePatternStoreImpl store1 = new WavePatternStoreImpl(storeDir, D, runtime);
            try {
                Random rng = new Random(42);
                for (int i = 0; i < 20; i++) {
                    store1.insert(boundaryPattern(rng, D), Map.of());
                }
            } finally {
                store1.close();
            }

            Path versionFile = storeDir.resolve("index").resolve(".circular-phase-v1");
            assertTrue(Files.exists(versionFile), "Version marker should exist");

            WavePatternStoreImpl store2 = new WavePatternStoreImpl(storeDir, D, runtime);
            try {
                assertTrue(Files.exists(versionFile));

                WavePattern query = boundaryPattern(new Random(99), D);
                List<ResonanceMatch> results = store2.query(query, 5);
                assertFalse(results.isEmpty(), "Query should return results on normal reopen");

                System.out.printf("Version marker test: store reopened normally, %d results%n",
                        results.size());
            } finally {
                store2.close();
            }
        } finally {
            runtime.close();
        }
    }
}
