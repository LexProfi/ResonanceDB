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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that close() completes promptly even when a background
 * index rebuild (Vamana) is in progress. Cooperative cancellation
 * must cause the build to exit without corrupting state.
 */
@Tag("benchmark")
class CloseAfterRebuildTest {

    private static final int DIM = 1536;
    private static final long SEED = 42L;

    /**
     * Insert enough data to trigger a background rebuild via delta threshold,
     * then immediately close the store. Must finish within 35 seconds.
     * Repeated 10 times for stability.
     */
    @RepeatedTest(10)
    @Timeout(value = 35, unit = TimeUnit.SECONDS)
    @DisplayName("close() after background rebuild completes within 35s")
    void closeAfterBackgroundRebuild(@TempDir Path tmpDir) {
        // Low delta threshold to trigger rebuild quickly
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.delta.maxSize", "50");
        System.setProperty("resonance.index.l2.enabled", "true");
        System.setProperty("resonance.index.l2.R", "16");
        System.setProperty("resonance.index.l2.Lbuild", "32");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            Path storeDir = tmpDir.resolve("close-test");
            WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

            Random rng = new Random(SEED);

            // Insert enough patterns to exceed delta threshold and trigger rebuild
            for (int i = 0; i < 200; i++) {
                double[] amp = new double[DIM];
                double[] phase = new double[DIM];
                for (int d = 0; d < DIM; d++) {
                    amp[d] = rng.nextDouble();
                    phase[d] = rng.nextDouble() * 2 * Math.PI;
                }
                try {
                    store.insert(new WavePattern(amp, phase), Map.of());
                } catch (Exception e) {
                    // skip duplicates
                }
            }

            // Give scheduler a moment to pick up the rebuild task
            Thread.sleep(100);

            // close() must return promptly thanks to cooperative cancellation
            long t0 = System.nanoTime();
            store.close();
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

            System.out.printf("  close() took %d ms%n", elapsed);
            assertTrue(elapsed < 30_000, "close() took " + elapsed + " ms, expected < 30s");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted");
        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l2.R");
            System.clearProperty("resonance.index.l2.Lbuild");
            runtime.close();
        }
    }

    /**
     * Explicit forceIndexRebuild() followed by immediate close().
     * Ensures the blocking rebuild path also respects shutdown.
     */
    @Test
    @Timeout(value = 35, unit = TimeUnit.SECONDS)
    @DisplayName("close() after forceIndexRebuild completes within 35s")
    void closeAfterForceRebuild(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "true");
        System.setProperty("resonance.index.l2.R", "16");
        System.setProperty("resonance.index.l2.Lbuild", "32");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        try {
            Path storeDir = tmpDir.resolve("force-close-test");
            WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);

            Random rng = new Random(SEED);
            for (int i = 0; i < 500; i++) {
                double[] amp = new double[DIM];
                double[] phase = new double[DIM];
                for (int d = 0; d < DIM; d++) {
                    amp[d] = rng.nextDouble();
                    phase[d] = rng.nextDouble() * 2 * Math.PI;
                }
                try {
                    store.insert(new WavePattern(amp, phase), Map.of());
                } catch (Exception e) {
                    // skip
                }
            }

            // Start rebuild in background, then close immediately
            Thread rebuildThread = new Thread(() -> {
                try { store.forceIndexRebuild(); } catch (Exception e) { /* expected on close */ }
            });
            rebuildThread.start();

            // Give it a moment to start
            Thread.sleep(200);

            long t0 = System.nanoTime();
            store.close();
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

            System.out.printf("  close() after force rebuild took %d ms%n", elapsed);

            rebuildThread.join(10_000);
            assertFalse(rebuildThread.isAlive(), "Rebuild thread should have finished");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted");
        } finally {
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l2.R");
            System.clearProperty("resonance.index.l2.Lbuild");
            runtime.close();
        }
    }
}
