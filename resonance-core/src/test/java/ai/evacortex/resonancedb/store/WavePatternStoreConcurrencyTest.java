/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
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
import java.util.stream.IntStream;


@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class WavePatternStoreConcurrencyTest {

    private static final String PROP_PATTERN_LEN = "resonance.pattern.len";

    private static final int THREADS          = 16;
    private static final int OPS_PER_THREAD   = 40;
    private static final int VALIDATION_TOP_K = 5;
    private static final double EPS           = 1e-6;

    private WavePatternStoreImpl store;

    @TempDir Path tempDir;

    @BeforeEach
    void setUp() {
        store = new WavePatternStoreImpl(tempDir);
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
            store = null;
        }
        System.gc();
    }

    private static int len() {
        return Integer.getInteger(PROP_PATTERN_LEN, 1536);
    }

    @Test
    @Timeout(120)
    void smokeConcurrentReadWrite() throws Exception {

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch latch = new CountDownLatch(1);
        ConcurrentMap<String, WavePattern> confirmed = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        final int patternLen = len();

        class Worker implements Runnable {
            @Override
            public void run() {
                try {
                    latch.await();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < OPS_PER_THREAD; i++) {

                        WavePattern initial = WavePatternTestUtils.createConstantPattern(
                                rnd.nextDouble(), rnd.nextDouble(), patternLen);

                        final String oldId;
                        try {
                            oldId = store.insert(initial, Map.of());
                        } catch (DuplicatePatternException dup) {
                            continue;
                        } catch (Throwable t) {
                            errors.add(new AssertionError("insert failed", t));
                            continue;
                        }

                        if (rnd.nextDouble() < 0.10) {
                            try {
                                store.delete(oldId);
                            } catch (PatternNotFoundException ignore) {
                            } catch (Throwable t) {
                                errors.add(new AssertionError("delete failed", t));
                            }
                            continue;
                        }

                        WavePattern updated = WavePatternTestUtils.createConstantPattern(
                                rnd.nextDouble(), rnd.nextDouble(), patternLen);

                        final String newId;
                        try {
                            newId = store.replace(oldId, updated, Map.of());
                        } catch (Throwable ex) {
                            errors.add(new AssertionError("replace failed", ex));
                            continue;
                        }
                        if (oldId.equals(newId)) {
                            continue;
                        }

                        boolean visible = store.containsExactPattern(updated)
                                || store.query(updated, VALIDATION_TOP_K).stream().anyMatch(h -> h.id().equals(newId));

                        if (!visible) {
                            visible = waitUntilPatternVisible(store, updated, VALIDATION_TOP_K);
                        }

                        if (visible) {
                            confirmed.put(newId, updated);
                        } else {
                            errors.add(new AssertionError("pattern not visible: " + newId));
                        }
                    }
                } catch (Throwable t) {
                    errors.add(t);
                }
            }
        }

        IntStream.range(0, THREADS).forEach(i -> pool.submit(new Worker()));

        latch.countDown();
        pool.shutdown();

        if (!pool.awaitTermination(90, TimeUnit.SECONDS)) {
            Assertions.fail("worker threads timeout");
        }

        Assertions.assertTrue(errors.isEmpty(), () -> {
            StringBuilder sb = new StringBuilder("Errors:\n");
            errors.forEach(e -> sb.append(e).append('\n'));
            return sb.toString();
        });

        confirmed.entrySet().removeIf(e ->
                store.query(e.getValue(), VALIDATION_TOP_K).stream()
                        .noneMatch(h -> h.id().equals(e.getKey()))
        );

        confirmed.forEach((id, pat) -> {
            Assertions.assertTrue(
                    store.query(pat, VALIDATION_TOP_K).stream().anyMatch(h -> h.id().equals(id)),
                    "query miss " + id
            );
            Assertions.assertTrue(store.containsExactPattern(pat), "manifest miss " + id);
        });

        WavePattern a = WavePatternTestUtils.createConstantPattern(.2, .3, patternLen);
        WavePattern b = WavePatternTestUtils.createConstantPattern(.4, .1, patternLen);
        Assertions.assertEquals(store.compare(a, b), store.compare(b, a), EPS);
        Assertions.assertEquals(VALIDATION_TOP_K, store.queryDetailed(a, VALIDATION_TOP_K).size());
        Assertions.assertEquals(
                VALIDATION_TOP_K,
                store.queryComposite(List.of(a, b), List.of(1.0, 1.0), VALIDATION_TOP_K).size());

        store.close();
        store = null;

        store = new WavePatternStoreImpl(tempDir);

        confirmed.forEach((id, pat) ->
                Assertions.assertTrue(store.containsExactPattern(pat), "not restored " + id)
        );
    }

    private static boolean waitUntilPatternVisible(WavePatternStoreImpl store, WavePattern expected, int topK)
            throws InterruptedException {

        String id = HashingUtil.computeContentHash(expected);

        for (int i = 0; i < 10; i++) {
            if (store.query(expected, topK).stream().anyMatch(h -> h.id().equals(id))) {
                return true;
            }
            Thread.sleep(1);
        }
        return false;
    }
}