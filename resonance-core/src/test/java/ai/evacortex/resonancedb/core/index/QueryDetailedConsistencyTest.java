/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consistency test: query() and queryDetailed() must return substantially
 * overlapping top-K results when both use the IVF two-phase path.
 *
 * <p>Divergences are expected due to float32 approximate scoring in Phase 1
 * vs double64 exact kernel with ampF in Phase 2 (queryDetailed uses
 * compareWithPhaseDelta, query uses compare). The test measures the
 * divergence rate and asserts it stays within acceptable bounds.</p>
 *
 * <p>Also tests both entry paths: fresh build and load-from-disk (via
 * rebuildSidecar after reopen).</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("benchmark")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class QueryDetailedConsistencyTest {

    private static final int DIM = 1536;
    private static final int N = 2000;
    private static final int NUM_QUERIES = 100;
    private static final int TOP_K = 10;
    private static final long SEED = 314159L;

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

    @Test
    @DisplayName("query() vs queryDetailed() consistency after fresh build")
    void consistencyAfterFreshBuild() {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.delta.maxSize", String.valueOf(N + 1));

        Path dir = tempDir.resolve("consistency-fresh");
        WavePatternStoreImpl store = new WavePatternStoreImpl(dir, DIM, runtime);

        try {
            insertData(store);

            long buildStart = System.nanoTime();
            store.forceIndexRebuild();
            System.out.printf("  index build: %d ms%n",
                    (System.nanoTime() - buildStart) / 1_000_000);

            double[] metrics = measureConsistency(store, "fresh build");
            double overlapRate = metrics[0];
            double detailedP50 = metrics[1];

            System.out.printf("  queryDetailed p50: %.1f ms%n", detailedP50);
            assertTrue(overlapRate >= 0.50,
                    "query/queryDetailed overlap should be ≥50%, got " + overlapRate);

        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
            try { store.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    @Test
    @DisplayName("query() vs queryDetailed() consistency after reopen + rebuildSidecar")
    void consistencyAfterReopen() {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.delta.maxSize", String.valueOf(N + 1));

        Path dir = tempDir.resolve("consistency-reopen");

        try {
            {
                WavePatternStoreImpl store = new WavePatternStoreImpl(dir, DIM, runtime);
                insertData(store);
                store.forceIndexRebuild();
                store.close();
            }

            {
                WavePatternStoreImpl store = new WavePatternStoreImpl(dir, DIM, runtime);
                try {
                    double[] metrics = measureConsistency(store, "after reopen");
                    double overlapRate = metrics[0];
                    double detailedP50 = metrics[1];

                    System.out.printf("  queryDetailed p50 (reopened): %.1f ms%n", detailedP50);
                    assertTrue(overlapRate >= 0.50,
                            "query/queryDetailed overlap after reopen should be ≥50%, got " + overlapRate);

                } finally {
                    try { store.close(); } catch (Exception e) { /* ignore */ }
                }
            }
        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
        }
    }

    private void insertData(WavePatternStoreImpl store) {
        Random rng = new Random(SEED);
        for (int i = 0; i < N; i++) {
            double[] amp = new double[DIM];
            double[] phase = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                amp[d] = rng.nextDouble();
                phase[d] = rng.nextDouble() * 2 * Math.PI;
            }
            try {
                store.insert(new WavePattern(amp, phase), Map.of());
            } catch (Exception e) { /* skip dups */ }
        }
    }

    /**
     * Measures overlap between query() and queryDetailed() results.
     *
     * @return [overlapRate, detailedP50ms]
     */
    private double[] measureConsistency(WavePatternStoreImpl store, String label) {
        Random qRng = new Random(SEED + 999);
        double totalOverlap = 0;
        long[] detailedLatencies = new long[NUM_QUERIES];
        int fallbackCount = 0;

        for (int q = 0; q < NUM_QUERIES; q++) {
            double[] amp = new double[DIM];
            double[] phase = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                amp[d] = qRng.nextDouble();
                phase[d] = qRng.nextDouble() * 2 * Math.PI;
            }
            WavePattern query = new WavePattern(amp, phase);

            List<ResonanceMatch> queryResults = store.query(query, TOP_K);
            Set<String> queryIds = new LinkedHashSet<>();
            for (ResonanceMatch m : queryResults) queryIds.add(m.id());

            long t0 = System.nanoTime();
            List<ResonanceMatchDetailed> detailedResults = store.queryDetailed(query, TOP_K);
            detailedLatencies[q] = System.nanoTime() - t0;

            Set<String> detailedIds = new LinkedHashSet<>();
            for (ResonanceMatchDetailed m : detailedResults) detailedIds.add(m.id());

            int overlap = 0;
            for (String id : queryIds) {
                if (detailedIds.contains(id)) overlap++;
            }
            int unionSize = Math.max(1, queryIds.size());
            totalOverlap += (double) overlap / unionSize;
        }

        double overlapRate = totalOverlap / NUM_QUERIES;

        Arrays.sort(detailedLatencies);
        double p50 = detailedLatencies[(int)(NUM_QUERIES * 0.50)] / 1_000_000.0;
        double p99 = detailedLatencies[(int)(NUM_QUERIES * 0.99)] / 1_000_000.0;

        System.out.printf("  [%s] overlap rate: %.4f, queryDetailed p50=%.1f ms, p99=%.1f ms%n",
                label, overlapRate, p50, p99);

        return new double[] { overlapRate, p50 };
    }
}
