/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.WavePatternTestUtils;
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;


@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class WavePatternStoreConcurrencyTest {

    private static final int THREADS        = 16;
    private static final int OPS_PER_THREAD = 40;
    private WavePatternStoreImpl store;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        store = new WavePatternStoreImpl(tempDir);
    }

    @AfterEach
    void tearDown() {
        store.close();
        System.gc();
    }

    @Test
    @Timeout(value = 120)
    void smokeConcurrentReadWrite() throws Exception {

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch startSignal = new CountDownLatch(1);
        ConcurrentMap<String, WavePattern> finalRevision = new ConcurrentHashMap<>();
        ConcurrentMap<String, WavePattern> initialRevision = new ConcurrentHashMap<>();

        class Worker implements Runnable {
            @Override
            public void run() {
                try {
                    startSignal.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        WavePattern initial = WavePatternTestUtils.createConstantPattern(Math.random(), Math.random(), 1024);
                        final String id;
                        try {
                            id = store.insert(initial, Map.of());
                        } catch (DuplicatePatternException ex) {
                            continue;
                        }
                        initialRevision.put(id, initial);
                        WavePattern updated = WavePatternTestUtils.createConstantPattern(Math.random(), Math.random(), 1024);
                        store.update(id, updated, Map.of());
                        finalRevision.put(id, updated);

                        Assertions.assertFalse(store.query(updated, 1).isEmpty(),
                                "Freshly updated pattern must be searchable");
                    }
                } catch (Throwable t) {
                    throw new AssertionError("Worker failed", t);
                }
            }
        }

        for (int t = 0; t < THREADS; t++) pool.submit(new Worker());
        startSignal.countDown();
        pool.shutdown();
        Assertions.assertTrue(pool.awaitTermination(90, TimeUnit.SECONDS),
                "Threads did not finish in time");

        for (Map.Entry<String, WavePattern> e : finalRevision.entrySet()) {
            WavePattern pat = e.getValue();
            List<ResonanceMatch> hits = store.query(pat, 1);
            Assertions.assertFalse(hits.isEmpty(), "Pattern not found after concurrent ops");
            ResonanceMatch top = hits.getFirst();
            float energy = store.compare(pat, top.pattern());
            Assertions.assertTrue(energy >= 0.99f,
                    "Pattern match is not exact (energy=" + energy + ')');
        }

        for (Map.Entry<String, WavePattern> e : initialRevision.entrySet()) {
            String id = e.getKey();
            WavePattern old = e.getValue();
            List<ResonanceMatch> hits = store.query(old, 1);
            if (hits.isEmpty()) continue;

            ResonanceMatch top = hits.getFirst();
            if (top.id().equals(id)) {
                float energy = store.compare(old, top.pattern());
                System.out.println("ID=" + id + " similarity(old→updated) = " + energy);
            } else {
                float energy = store.compare(old, top.pattern());
                System.out.printf("Unrelated pattern matched strongly (old ID=%s, matched ID=%s, energy=%.6f)%n",
                        id, top.id(), energy);
            }
        }
    }
}