/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.io.CachedReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for CachedReader lifecycle: concurrent acquire/release,
 * close-while-active, no double-release, reads during eviction.
 */
class CachedReaderStressTest {

    private static final int DIM = 64;
    private static final int PATTERN_COUNT = 50;

    @TempDir
    Path tempDir;

    private Path segmentPath;
    private List<String> patternIds;

    @BeforeEach
    void writeSegment() throws Exception {
        segmentPath = tempDir.resolve("stress-seg.rdb");
        patternIds = new ArrayList<>();
        Random rng = new Random(42);

        try (SegmentWriter writer = new SegmentWriter(segmentPath)) {
            for (int i = 0; i < PATTERN_COUNT; i++) {
                double[] amp = new double[DIM];
                double[] phase = new double[DIM];
                for (int d = 0; d < DIM; d++) {
                    amp[d] = rng.nextDouble();
                    phase[d] = rng.nextDouble() * 2 * Math.PI;
                }
                WavePattern p = new WavePattern(amp, phase);
                String id = HashingUtil.computeContentHash(p);
                writer.write(id, p);
                patternIds.add(id);
            }
            writer.flush();
        }
    }

    @Test
    @DisplayName("CachedReader: concurrent acquire/release from multiple threads")
    void concurrentAcquireRelease() throws Exception {
        CachedReader reader = CachedReader.open(segmentPath);
        int threads = 8;
        int opsPerThread = 500;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            exec.submit(() -> {
                try {
                    Random rng = new Random(Thread.currentThread().getId());
                    for (int i = 0; i < opsPerThread; i++) {
                        reader.acquire();
                        try {
                            String id = patternIds.get(rng.nextInt(patternIds.size()));
                            WavePattern p = reader.readById(id);
                            assertNotNull(p);
                            assertEquals(DIM, p.amplitude().length);
                        } finally {
                            reader.release();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Threads should complete within timeout");
        exec.shutdown();
        assertEquals(0, errors.get(), "No errors during concurrent acquire/release");
        reader.close();
    }

    @Test
    @DisplayName("CachedReader: close while active readers hold acquire")
    void closeWhileActive() throws Exception {
        CachedReader reader = CachedReader.open(segmentPath);
        int activeReaders = 4;
        CountDownLatch readersReady = new CountDownLatch(activeReaders);
        CountDownLatch closeHappened = new CountDownLatch(1);
        CountDownLatch readersDone = new CountDownLatch(activeReaders);
        AtomicInteger errors = new AtomicInteger(0);
        ExecutorService exec = Executors.newFixedThreadPool(activeReaders + 1);

        for (int t = 0; t < activeReaders; t++) {
            final int tid = t;
            exec.submit(() -> {
                try {
                    reader.acquire();
                    readersReady.countDown();
                    assertTrue(closeHappened.await(10, TimeUnit.SECONDS));
                    String id = patternIds.get(tid % patternIds.size());
                    WavePattern p = reader.readById(id);
                    assertNotNull(p, "read should work while acquire is held even after close");
                    reader.release();
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    readersDone.countDown();
                }
            });
        }

        assertTrue(readersReady.await(10, TimeUnit.SECONDS));
        reader.close();
        closeHappened.countDown();

        assertTrue(readersDone.await(10, TimeUnit.SECONDS), "Readers should complete after close");
        exec.shutdown();
        assertEquals(0, errors.get(), "No errors during close-while-active");
    }

    @Test
    @DisplayName("CachedReader: acquire after close throws IllegalStateException")
    void acquireAfterClose() throws Exception {
        CachedReader reader = CachedReader.open(segmentPath);
        reader.close();
        assertThrows(IllegalStateException.class, reader::acquire,
                "acquire() on closed reader should throw");
    }

    @Test
    @DisplayName("CachedReader: double release throws IllegalStateException")
    void doubleRelease() throws Exception {
        CachedReader reader = CachedReader.open(segmentPath);
        reader.acquire();
        reader.release();
        assertThrows(IllegalStateException.class, reader::release,
                "second release should throw (refCount below zero)");
        reader.close();
    }

    @Test
    @DisplayName("CachedReader: rapid interleaved acquire/release/close cycles")
    void rapidCycles() throws Exception {
        int cycles = 20;
        int threadsPerCycle = 4;
        int opsPerThread = 100;

        for (int cycle = 0; cycle < cycles; cycle++) {
            CachedReader reader = CachedReader.open(segmentPath);
            ExecutorService exec = Executors.newFixedThreadPool(threadsPerCycle);
            AtomicInteger errors = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(threadsPerCycle);

            for (int t = 0; t < threadsPerCycle; t++) {
                exec.submit(() -> {
                    try {
                        Random rng = new Random(Thread.currentThread().getId());
                        for (int i = 0; i < opsPerThread; i++) {
                            reader.acquire();
                            try {
                                String id = patternIds.get(rng.nextInt(patternIds.size()));
                                assertTrue(reader.contains(id));
                            } finally {
                                reader.release();
                            }
                        }
                    } catch (IllegalStateException e) {
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            Thread.sleep(1);
            reader.close();

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            exec.shutdown();
            assertEquals(0, errors.get(), "No unexpected errors in cycle " + cycle);
        }
    }

    @Test
    @DisplayName("CachedReader: readPatternFlat with acquire/release concurrent with close")
    void flatReadConcurrentClose() throws Exception {
        CachedReader reader = CachedReader.open(segmentPath);
        int threads = 4;
        int opsPerThread = 200;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            exec.submit(() -> {
                try {
                    Random rng = new Random(Thread.currentThread().getId());
                    double[] amp = new double[DIM];
                    double[] phase = new double[DIM];
                    for (int i = 0; i < opsPerThread; i++) {
                        try {
                            reader.acquire();
                        } catch (IllegalStateException e) {
                            break;
                        }
                        try {
                            String id = patternIds.get(rng.nextInt(patternIds.size()));
                            boolean ok = reader.readPatternFlat(id, amp, 0, phase, 0, DIM);
                            if (ok) successCount.incrementAndGet();
                        } finally {
                            reader.release();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        Thread.sleep(5);
        reader.close();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        exec.shutdown();
        assertEquals(0, errors.get(), "No crashes during flat read + close");
        assertTrue(successCount.get() > 0, "At least some flat reads succeeded before close");
    }

    @Test
    @DisplayName("CachedReader: allIds and contains are consistent")
    void idsConsistency() throws Exception {
        CachedReader reader = CachedReader.open(segmentPath);
        Set<String> allIds = reader.allIds();
        assertEquals(PATTERN_COUNT, allIds.size());
        for (String id : patternIds) {
            assertTrue(reader.contains(id), "inserted ID should be present: " + id);
        }
        reader.close();
    }

    @Test
    @DisplayName("CachedReader: samplePatternLength returns correct dimension")
    void sampleLength() throws Exception {
        CachedReader reader = CachedReader.open(segmentPath);
        OptionalInt len = reader.samplePatternLength();
        assertTrue(len.isPresent());
        assertEquals(DIM, len.getAsInt());
        reader.close();
    }
}
