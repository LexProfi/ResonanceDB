/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.HashingUtil;
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

    private static final int THREADS          = 16;
    private static final int OPS_PER_THREAD   = 40;
    private static final int VALIDATION_TOP_K = 5;
    private static final double EPS           = 1e-6;

    private WavePatternStoreImpl store;

    @TempDir Path tempDir;

    @BeforeEach void setUp() { store = new WavePatternStoreImpl(tempDir); }
    @AfterEach  void tearDown() { store.close(); System.gc(); }

    @Test @Timeout(120)
    void smokeConcurrentReadWrite() throws Exception {

        ExecutorService  pool   = Executors.newFixedThreadPool(THREADS);
        CountDownLatch   latch  = new CountDownLatch(1);
        ConcurrentMap<String,WavePattern> confirmed = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<Throwable>  errors    = new ConcurrentLinkedQueue<>();

        class Worker implements Runnable {
            @Override public void run() {
                try {
                    latch.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {

                        WavePattern initial = WavePatternTestUtils.createConstantPattern(
                                Math.random(), Math.random(), 1024);
                        final String oldId;
                        try { oldId = store.insert(initial, Map.of()); }
                        catch (DuplicatePatternException dup) { continue; }

                        if (ThreadLocalRandom.current().nextDouble() < .10) {
                            try { store.delete(oldId); } catch (PatternNotFoundException ignore) {}
                            continue;
                        }

                        WavePattern updated = WavePatternTestUtils.createConstantPattern(
                                Math.random(), Math.random(), 1024);
                        final String newId;
                        try { newId = store.replace(oldId, updated, Map.of()); }
                        catch (Throwable ex) {
                            errors.add(new AssertionError("replace failed", ex));
                            continue;
                        }

                        if (!oldId.equals(newId)) {
                            boolean visible = store.containsExactPattern(updated) ||
                                    store.query(updated, VALIDATION_TOP_K)
                                            .stream().anyMatch(h -> h.id().equals(newId));

                            if (!visible)
                                visible = waitUntilPatternVisible(store, updated);

                            if (visible) confirmed.put(newId, updated);
                            else errors.add(new AssertionError("pattern not visible: "+newId));
                        }
                    }
                } catch (Throwable t) { errors.add(t); }
            }
        }
        IntStream.range(0, THREADS).forEach(_ -> pool.submit(new Worker()));

        latch.countDown();
        pool.shutdown();

        if (!pool.awaitTermination(90, TimeUnit.SECONDS))
            Assertions.fail("worker threads timeout");
        Assertions.assertTrue(errors.isEmpty(), () -> {
            StringBuilder sb=new StringBuilder("Errors:\n");
            errors.forEach(e->sb.append(e).append('\n')); return sb.toString(); });
        confirmed.entrySet().removeIf(e ->
                store.query(e.getValue(), VALIDATION_TOP_K).stream()
                        .noneMatch(h -> h.id().equals(e.getKey()))
        );

        confirmed.forEach((id, pat) -> {
            Assertions.assertTrue(
                    store.query(pat, VALIDATION_TOP_K).stream()
                            .anyMatch(h -> h.id().equals(id)), "query miss "+id);
            Assertions.assertTrue(store.containsExactPattern(pat),"manifest miss "+id);
        });

        WavePattern a = WavePatternTestUtils.createConstantPattern(.2,.3,1024);
        WavePattern b = WavePatternTestUtils.createConstantPattern(.4,.1,1024);
        Assertions.assertEquals(store.compare(a,b), store.compare(b,a), EPS);

        Assertions.assertEquals(5, store.queryDetailed(a,5).size());
        Assertions.assertEquals(5, store.queryComposite(List.of(a,b),null,5).size());

        store.close();
        store = new WavePatternStoreImpl(tempDir);
        confirmed.forEach((id,pat) ->
                Assertions.assertTrue(store.containsExactPattern(pat),"not restored "+id));
    }

    private static boolean waitUntilPatternVisible(WavePatternStoreImpl store, WavePattern expected)
            throws InterruptedException {

        String id = HashingUtil.computeContentHash(expected);
        for (int i = 0; i< 10; i++) {
            if (store.query(expected, VALIDATION_TOP_K).stream()
                    .anyMatch(h->h.id().equals(id))) return true;
            Thread.sleep(1);
        }
        return false;
    }
}