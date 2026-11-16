/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrentExactStressTest {

    private static final int DIM = 64;
    private static final int INITIAL_PATTERNS = 200;
    private static final int TOP_K = 10;
    private static final int OPS_PER_THREAD = 100;
    private static final long TIMEOUT_SECONDS = 60;

    private final String[] MANAGED_PROPS = {
            "resonance.index.enabled",
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

    private static WavePattern randomPattern(Random rng) {
        double[] amp = new double[DIM];
        double[] phase = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    @Test
    @DisplayName("Concurrent exact queries + inserts + deletes — no exceptions, no deadlocks")
    void concurrentExactOps(@TempDir Path tmpDir) throws Exception {
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.delta.sealThreshold", "100");
        System.setProperty("resonance.delta.sealIntervalSeconds", "9999");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("concurrent"), DIM, runtime);
        try {
            Random seedRng = new Random(42L);
            ConcurrentLinkedQueue<String> knownIds = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < INITIAL_PATTERNS; i++) {
                WavePattern p = randomPattern(seedRng);
                try {
                    String id = store.insert(p, Map.of());
                    knownIds.add(id);
                } catch (DuplicatePatternException ignored) { /* dup */ }
            }

            AtomicReference<Throwable> firstError = new AtomicReference<>();
            AtomicInteger queryCount = new AtomicInteger();
            AtomicInteger insertCount = new AtomicInteger();
            AtomicInteger deleteCount = new AtomicInteger();

            int threadCount = 4;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                Thread worker = new Thread(() -> {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    Random rng = new Random(1000L + threadId);
                    try {
                        for (int op = 0; op < OPS_PER_THREAD; op++) {
                            if (firstError.get() != null) return;

                            double roll = rng.nextDouble();
                            if (roll < 0.5) {
                                WavePattern query = randomPattern(rng);
                                List<ResonanceMatch> results = store.query(query, TOP_K);
                                assertTrue(results.size() <= TOP_K,
                                        "results must not exceed topK");
                                queryCount.incrementAndGet();
                            } else if (roll < 0.8) {
                                WavePattern p = randomPattern(rng);
                                try {
                                    String id = store.insert(p, Map.of());
                                    knownIds.add(id);
                                    insertCount.incrementAndGet();
                                } catch (DuplicatePatternException ignored) { /* dup */ }
                            } else {
                                String id = knownIds.poll();
                                if (id != null) {
                                    try {
                                        store.delete(id);
                                        deleteCount.incrementAndGet();
                                    } catch (PatternNotFoundException ignored) { /* already deleted */ }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        firstError.compareAndSet(null, e);
                    } finally {
                        doneLatch.countDown();
                    }
                }, "stress-" + threadId);
                worker.setDaemon(true);
                worker.start();
            }

            startLatch.countDown();
            boolean finished = doneLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertTrue(finished, "all threads must finish within " + TIMEOUT_SECONDS + "s (deadlock?)");

            if (firstError.get() != null) {
                fail("thread threw exception: " + firstError.get(), firstError.get());
            }

            assertTrue(queryCount.get() > 0, "must have executed queries");
            assertTrue(insertCount.get() > 0, "must have executed inserts");
        } finally {
            store.close();
            runtime.close();
        }
    }

    @Test
    @DisplayName("Parallel exact queries return consistent sizes")
    void parallelQueryConsistency(@TempDir Path tmpDir) throws Exception {
        System.setProperty("resonance.index.enabled", "false");
        System.setProperty("resonance.delta.sealThreshold", "5000");
        System.setProperty("resonance.delta.sealIntervalSeconds", "9999");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("parallel-query"), DIM, runtime);
        try {
            Random rng = new Random(314159L);
            for (int i = 0; i < INITIAL_PATTERNS; i++) {
                WavePattern p = randomPattern(rng);
                try {
                    store.insert(p, Map.of());
                } catch (DuplicatePatternException ignored) { /* dup */ }
            }

            WavePattern query = randomPattern(new Random(42L));
            int parallelism = 8;
            ExecutorService exec = Executors.newFixedThreadPool(parallelism);
            try {
                List<Future<List<String>>> futures = new ArrayList<>();
                for (int i = 0; i < parallelism; i++) {
                    futures.add(exec.submit(() -> {
                        List<ResonanceMatch> results = store.query(query, TOP_K);
                        return results.stream().map(ResonanceMatch::id).toList();
                    }));
                }

                List<String> reference = null;
                for (Future<List<String>> f : futures) {
                    List<String> result = f.get(30, TimeUnit.SECONDS);
                    assertNotNull(result);
                    assertTrue(result.size() <= TOP_K);
                    if (reference == null) {
                        reference = result;
                    } else {
                        assertEquals(reference, result,
                                "parallel queries with same input must return identical results");
                    }
                }
            } finally {
                exec.shutdownNow();
            }
        } finally {
            store.close();
            runtime.close();
        }
    }
}
