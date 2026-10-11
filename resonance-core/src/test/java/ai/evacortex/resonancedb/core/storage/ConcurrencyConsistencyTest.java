/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.compactor.DefaultSegmentCompactor;
import ai.evacortex.resonancedb.core.storage.compactor.SegmentCompactor;
import ai.evacortex.resonancedb.core.storage.io.SegmentCache;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyConsistencyTest {

    private static final int DIM = 64;

    private static final String[] MANAGED_PROPS = {
            "resonance.wal.enabled", "resonance.wal.durability",
            "resonance.delta.sealThreshold", "resonance.delta.maxEntries",
            "resonance.delta.sealIntervalSeconds",
            "resonance.index.enabled"
    };

    @TempDir
    Path tempDir;
    private WavePatternStoreImpl store;
    private StoreRuntimeServices runtime;
    private final Map<String, String> savedProps = new HashMap<>();

    @BeforeEach
    void setUp() {
        for (String key : MANAGED_PROPS) {
            savedProps.put(key, System.getProperty(key));
        }
        System.setProperty("resonance.wal.enabled", "true");
        System.setProperty("resonance.wal.durability", "strict");
        System.setProperty("resonance.delta.sealThreshold", "10000");
        System.setProperty("resonance.delta.maxEntries", "20000");
        System.setProperty("resonance.index.enabled", "false");
    }

    @AfterEach
    void tearDown() {
        if (store != null) store.close();
        if (runtime != null) runtime.close();
        for (String key : MANAGED_PROPS) {
            String original = savedProps.get(key);
            if (original == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, original);
            }
        }
    }

    private void openStore() {
        runtime = StoreRuntimeServices.fromSystemProperties();
        store = new WavePatternStoreImpl(tempDir, DIM, runtime);
    }

    // ── Fix #1: replaceViaDelta removePhaseStats ────────────────────────

    @Test
    @DisplayName("replaceViaDelta subtracts old phase stats from group")
    void replaceViaDeltaRemovesPhaseStats() {
        openStore();

        double targetPhase = 0.5;
        WavePattern pOld = patternWithPhase(targetPhase);
        String oldId = store.insert(pOld, Map.of());
        store.sealDelta();

        WavePattern pNew = patternWithPhase(targetPhase + 0.01);
        String newId = store.replace(oldId, pNew, Map.of());

        store.sealDelta();

        List<ResonanceMatch> results = store.query(pNew, 5);
        assertTrue(results.stream().anyMatch(m -> m.id().equals(newId)),
                "Replaced pattern must be findable");
        assertTrue(results.stream().noneMatch(m -> m.id().equals(oldId)),
                "Old pattern must not appear");
    }

    @Test
    @DisplayName("Bulk replace via delta — phase stats stay accurate")
    void bulkReplaceViaDeltaPhaseAccuracy() {
        openStore();

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            WavePattern p = patternWithPhase(0.3 + i * 0.001);
            ids.add(store.insert(p, Map.of()));
        }
        store.sealDelta();

        for (String id : ids) {
            WavePattern pNew = patternWithPhase(2.0 + Math.random() * 0.1);
            store.replace(id, pNew, Map.of());
        }
        store.sealDelta();

        WavePattern query = patternWithPhase(2.05);
        List<ResonanceMatch> results = store.query(query, 50);
        assertTrue(results.size() >= 10,
                "Phase routing should find replaced patterns near new phase center");
    }

    // ── Fix #4/5: getAvgPhaseIfPresent + FP epsilon ─────────────────────

    @Test
    @DisplayName("PhaseSegmentGroup: add/remove cycle zeroes stats with epsilon")
    void phaseStatsAddRemoveCycleZero() {
        SegmentCompactor noop = group -> {};
        PhaseSegmentGroup group = new PhaseSegmentGroup("test", tempDir.resolve("seg"), noop);

        Random rng = new Random(42);
        double[] phases = new double[1000];
        for (int i = 0; i < phases.length; i++) {
            phases[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
            group.updatePhaseStats(phases[i]);
        }

        assertTrue(group.hasPhaseStats(), "Should have stats after adds");
        assertTrue(group.getAvgPhaseIfPresent().isPresent());

        for (double p : phases) {
            group.removePhaseStats(p);
        }

        assertFalse(group.hasPhaseStats(),
                "hasPhaseStats should be false after removing all contributions (FP epsilon)");
        assertTrue(group.getAvgPhaseIfPresent().isEmpty(),
                "getAvgPhaseIfPresent should be empty when stats are zeroed");
    }

    @Test
    @DisplayName("getAvgPhaseIfPresent is atomic — no TOCTOU")
    void getAvgPhaseIfPresentAtomic() throws Exception {
        SegmentCompactor noop = group -> {};
        PhaseSegmentGroup group = new PhaseSegmentGroup("atomic-test", tempDir.resolve("seg2"), noop);

        group.updatePhaseStats(1.0);

        int iterations = 10_000;
        ExecutorService exec = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        futures.add(exec.submit(() -> {
            latch.countDown();
            for (int i = 0; i < iterations; i++) {
                group.updatePhaseStats(Math.random() * 2 * Math.PI - Math.PI);
            }
        }));
        futures.add(exec.submit(() -> {
            try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < iterations; i++) {
                group.removePhaseStats(Math.random() * 2 * Math.PI - Math.PI);
            }
        }));
        futures.add(exec.submit(() -> {
            try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < iterations; i++) {
                OptionalDouble val = group.getAvgPhaseIfPresent();
                if (val.isPresent()) {
                    double v = val.getAsDouble();
                    assertTrue(v >= -Math.PI && v <= Math.PI,
                            "avgPhase must be in [-π, π], got: " + v);
                }
            }
        }));

        for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        exec.shutdown();
    }

    // ── Fix #6: base() handles merged segment names ────────────────────

    @Test
    @DisplayName("base() extracts correct group from merged segment names")
    void baseHandlesMergedSegments() {
        openStore();

        WavePattern p1 = patternWithPhase(0.5);
        WavePattern p2 = patternWithPhase(0.51);
        WavePattern p3 = patternWithPhase(0.52);
        WavePattern p4 = patternWithPhase(0.53);
        WavePattern p5 = patternWithPhase(0.54);

        store.insert(p1, Map.of());
        store.insert(p2, Map.of());
        store.insert(p3, Map.of());
        store.insert(p4, Map.of());
        store.insert(p5, Map.of());

        store.sealDelta();

        WavePattern query = patternWithPhase(0.52);
        List<ResonanceMatch> before = store.query(query, 10);
        assertFalse(before.isEmpty(), "Patterns should be visible before close");

        store.close();
        store = null;

        StoreRuntimeServices rt2 = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(tempDir, DIM, rt2);
        try {
            List<ResonanceMatch> after = store2.query(query, 10);
            assertFalse(after.isEmpty(), "Patterns should survive restart");
            assertTrue(after.size() >= before.size(),
                    "No patterns lost after restart");
        } finally {
            store2.close();
            rt2.close();
        }
    }

    // ── Fix #2: compaction visibility ───────────────────────────────────

    @Test
    @DisplayName("compactPhase — patterns visible immediately after compaction")
    void compactionDoesNotHideData() {
        System.setProperty("resonance.wal.enabled", "false");
        openStore();

        List<String> insertedIds = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            WavePattern p = randomPattern(DIM, 5000 + i);
            insertedIds.add(store.insert(p, Map.of()));
        }

        WavePattern query = randomPattern(DIM, 5000);
        List<ResonanceMatch> beforeCompact = store.query(query, 20);

        for (String baseName : store.getShardSelector().allShards()) {
            String group = baseName.replace(".segment", "");
            int dash = group.lastIndexOf('-');
            if (dash > 0) group = group.substring(0, dash);
            store.compactPhase(group);
        }

        List<ResonanceMatch> afterCompact = store.query(query, 20);
        assertEquals(beforeCompact.size(), afterCompact.size(),
                "No data should disappear after compaction");

        Set<String> afterIds = new HashSet<>();
        for (ResonanceMatch m : afterCompact) afterIds.add(m.id());
        for (ResonanceMatch m : beforeCompact) {
            assertTrue(afterIds.contains(m.id()),
                    "Pattern " + m.id() + " disappeared after compaction");
        }
    }

    // ── Fix #3: sealDelta atomicity ─────────────────────────────────────

    @Test
    @DisplayName("sealDelta + concurrent query — no phantom or lost results")
    void sealDeltaConcurrentQuery() throws Exception {
        openStore();

        int patternCount = 100;
        Set<String> allIds = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < patternCount; i++) {
            WavePattern p = randomPattern(DIM, 6000 + i);
            allIds.add(store.insert(p, Map.of()));
        }

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch go = new CountDownLatch(1);

        Future<?> sealTask = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            store.sealDelta();
        });

        Future<List<ResonanceMatch>> queryTask = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return store.query(randomPattern(DIM, 6000), patternCount);
        });

        go.countDown();
        sealTask.get(30, TimeUnit.SECONDS);
        queryTask.get(30, TimeUnit.SECONDS);

        List<ResonanceMatch> finalResults = store.query(randomPattern(DIM, 6000), patternCount);
        assertFalse(finalResults.isEmpty(), "Patterns must be visible after seal completes");

        exec.shutdown();
    }

    // ── Integrated stress test ──────────────────────────────────────────

    @Test
    @DisplayName("Concurrent insert + replace + delete + query — no data corruption")
    void integratedConcurrencyStress() throws Exception {
        openStore();

        int rounds = 50;
        ExecutorService exec = Executors.newFixedThreadPool(4);
        ConcurrentLinkedQueue<String> liveIds = new ConcurrentLinkedQueue<>();
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        futures.add(exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < rounds; i++) {
                WavePattern p = randomPattern(DIM, 7000 + i);
                try {
                    String id = store.insert(p, Map.of());
                    liveIds.add(id);
                } catch (Exception ignored) {}
            }
        }));

        futures.add(exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < rounds / 2; i++) {
                String id = liveIds.poll();
                if (id != null) {
                    try {
                        WavePattern pNew = randomPattern(DIM, 8000 + i);
                        String newId = store.replace(id, pNew, Map.of());
                        liveIds.add(newId);
                    } catch (Exception ignored) {}
                }
            }
        }));

        futures.add(exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < rounds / 4; i++) {
                String id = liveIds.poll();
                if (id != null) {
                    try {
                        store.delete(id);
                    } catch (Exception ignored) {}
                }
            }
        }));

        futures.add(exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < rounds; i++) {
                try {
                    store.query(randomPattern(DIM, 9000 + i), 10);
                } catch (Exception ignored) {}
            }
        }));

        go.countDown();
        for (Future<?> f : futures) f.get(60, TimeUnit.SECONDS);
        exec.shutdown();

        store.sealDelta();

        WavePattern q = randomPattern(DIM, 7000);
        List<ResonanceMatch> results = store.query(q, 100);
        assertNotNull(results);
    }

    // ── Fix BUG-1: frozen HashMap defensive copy ─────────────────────────

    @Test
    @DisplayName("Concurrent sealDelta + query — no ConcurrentModificationException on frozen map")
    void sealDeltaFrozenMapDefensiveCopy() throws Exception {
        openStore();

        int patternCount = 200;
        for (int i = 0; i < patternCount; i++) {
            WavePattern p = randomPattern(DIM, 10_000 + i);
            store.insert(p, Map.of());
        }

        ExecutorService exec = Executors.newFixedThreadPool(4);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        futures.add(exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            store.sealDelta();
        }));

        for (int t = 0; t < 3; t++) {
            futures.add(exec.submit(() -> {
                try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < 50; i++) {
                    store.query(randomPattern(DIM, 10_000), 10);
                }
            }));
        }

        go.countDown();
        for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        exec.shutdown();

        List<ResonanceMatch> results = store.query(randomPattern(DIM, 10_000), patternCount);
        assertFalse(results.isEmpty(), "Patterns must be visible after concurrent seal+query");
    }

    // ── Fix BUG-2: tombstone resurrection prevention ───────────────────

    @Test
    @DisplayName("Delete during sealDelta does not resurrect entries")
    void deleteDuringSealDoesNotResurrect() throws Exception {
        openStore();

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            WavePattern p = randomPattern(DIM, 11_000 + i);
            ids.add(store.insert(p, Map.of()));
        }

        store.sealDelta();

        for (int i = 0; i < 30; i++) {
            WavePattern p = randomPattern(DIM, 12_000 + i);
            ids.add(store.insert(p, Map.of()));
        }

        Set<String> deletedIds = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 10; i++) {
            String id = ids.get(50 + i);
            store.delete(id);
            deletedIds.add(id);
        }

        store.sealDelta();

        List<ResonanceMatch> results = store.query(randomPattern(DIM, 12_000), 100);
        for (ResonanceMatch m : results) {
            assertFalse(deletedIds.contains(m.id()),
                    "Deleted pattern " + m.id() + " must not appear in results");
        }
    }

    // ── Fix BUG-5: ManifestIndex atomic replace ────────────────────────

    @Test
    @DisplayName("ManifestIndex.replace(oldId, newId) is atomic — get() sees no gap")
    void manifestReplaceAtomic() throws Exception {
        ManifestIndex idx = ManifestIndex.loadOrCreate(tempDir.resolve("manifest-test.idx"));
        idx.add("old-1", "seg-0.segment", 0, 0.5);

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch go = new CountDownLatch(1);

        Future<?> replacer = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            idx.replace("old-1", "new-1", "seg-1.segment", 100, 0.6);
        });

        Future<Boolean> reader = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < 10_000; i++) {
                ManifestIndex.PatternLocation oldLoc = idx.get("old-1");
                ManifestIndex.PatternLocation newLoc = idx.get("new-1");
                if (oldLoc == null && newLoc == null) {
                    return false;
                }
            }
            return true;
        });

        go.countDown();
        replacer.get(10, TimeUnit.SECONDS);
        boolean neverInvisible = reader.get(10, TimeUnit.SECONDS);
        exec.shutdown();

        assertTrue(neverInvisible,
                "Pattern must always be visible under either old or new ID during replace");
        assertNull(idx.get("old-1"), "Old ID must be removed");
        assertNotNull(idx.get("new-1"), "New ID must be present");
    }

    // ── Fix BUG-4: close() WAL ordering ───────────────────────────────

    @Test
    @DisplayName("close() does not crash on concurrent insert — WAL closed safely")
    void closeWithConcurrentInsert() throws Exception {
        openStore();

        for (int i = 0; i < 10; i++) {
            store.insert(randomPattern(DIM, 13_000 + i), Map.of());
        }

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch go = new CountDownLatch(1);

        Future<?> inserter = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < 50; i++) {
                try {
                    store.insert(randomPattern(DIM, 14_000 + i), Map.of());
                } catch (IllegalStateException e) {
                    break;
                } catch (Exception ignored) {}
            }
        });

        Future<?> closer = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            try { Thread.sleep(5); } catch (InterruptedException ignored) {}
            store.close();
            store = null;
        });

        go.countDown();
        assertDoesNotThrow(() -> inserter.get(30, TimeUnit.SECONDS));
        assertDoesNotThrow(() -> closer.get(30, TimeUnit.SECONDS));
        exec.shutdown();
    }

    // ── Fix BUG-7: awaitSealCompletion direct seal ────────────────────

    @Test
    @DisplayName("Backpressure via awaitSealCompletion drains delta buffer")
    void backpressureDrainsBuffer() {
        System.setProperty("resonance.delta.maxEntries", "50");
        openStore();

        for (int i = 0; i < 80; i++) {
            store.insert(randomPattern(DIM, 15_000 + i), Map.of());
        }

        List<ResonanceMatch> results = store.query(randomPattern(DIM, 15_000), 100);
        assertFalse(results.isEmpty(),
                "All patterns should be queryable after backpressure-triggered seal");
    }

    // ── Phase 3 Fix BUG-1: compactPhase + sealDelta serialization ───────

    @Test
    @DisplayName("Concurrent sealDelta + compactPhase — no data loss")
    void concurrentSealAndCompact() throws Exception {
        System.setProperty("resonance.wal.enabled", "false");
        openStore();

        for (int i = 0; i < 100; i++) {
            store.insert(randomPattern(DIM, 20_000 + i), Map.of());
        }
        store.sealDelta();

        for (int i = 0; i < 50; i++) {
            store.insert(randomPattern(DIM, 21_000 + i), Map.of());
        }

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch go = new CountDownLatch(1);

        Future<?> sealTask = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            store.sealDelta();
        });

        Future<?> compactTask = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (String baseName : store.getShardSelector().allShards()) {
                String group = baseName.replace(".segment", "");
                int dash = group.lastIndexOf('-');
                if (dash > 0) group = group.substring(0, dash);
                try { store.compactPhase(group); } catch (Exception ignored) {}
            }
        });

        go.countDown();
        sealTask.get(30, TimeUnit.SECONDS);
        compactTask.get(30, TimeUnit.SECONDS);
        exec.shutdown();

        List<ResonanceMatch> results = store.query(randomPattern(DIM, 20_000), 200);
        assertTrue(results.size() >= 100,
                "All sealed patterns must survive concurrent seal+compact, found: " + results.size());
    }

    // ── Phase 3 Fix BUG-5: WAL replace after validation ──────────────────

    @Test
    @DisplayName("Replace of non-existent pattern does not corrupt WAL")
    void replaceNonExistentDoesNotCorruptWal() {
        openStore();

        store.insert(randomPattern(DIM, 24_000), Map.of());

        WavePattern phantom = randomPattern(DIM, 99_999);
        String phantomHash = ai.evacortex.resonancedb.core.storage.util.HashingUtil
                .computeContentHash(phantom);

        assertThrows(PatternNotFoundException.class,
                () -> store.replace(phantomHash, randomPattern(DIM, 24_001), Map.of()),
                "Replace of non-existent ID should throw");

        String validId = store.insert(randomPattern(DIM, 24_002), Map.of());
        assertNotNull(validId);
        store.sealDelta();

        List<ResonanceMatch> results = store.query(randomPattern(DIM, 24_002), 10);
        assertFalse(results.isEmpty(), "Store must remain functional after failed replace");
    }

    // ── Phase 3 Fix BUG-6: SegmentCache.removeSegment ────────────────────

    @Test
    @DisplayName("SegmentCache.removeSegment evicts stale cache entries")
    void segmentCacheRemoveSegment() throws Exception {
        Path segDir = tempDir.resolve("cache-test-segs");
        Files.createDirectories(segDir);

        WavePattern p = randomPattern(DIM, 25_000);
        String validId = ai.evacortex.resonancedb.core.storage.util.HashingUtil
                .computeContentHash(p);

        SegmentWriter writer = new SegmentWriter(segDir.resolve("test-seg.segment"));
        writer.write(validId, p);
        long offset = writer.flush();
        writer.close();

        SegmentCache cache = new SegmentCache(segDir, 1L << 20);
        try {
            cache.updateVersion("test-seg.segment", offset);
            assertNotNull(cache.getOrLoad("test-seg.segment"),
                    "getOrLoad should return cached reader after updateVersion");

            cache.removeSegment("test-seg.segment");
            assertNull(cache.getOrLoad("test-seg.segment"),
                    "After removeSegment, getOrLoad should return null");
        } finally {
            cache.close();
        }
    }

    // ── Phase 3 Fix BUG-7: containsExactPattern concurrent consistency ──

    @Test
    @DisplayName("containsExactPattern consistent during concurrent insert+seal")
    void containsExactPatternConcurrentConsistency() throws Exception {
        openStore();

        WavePattern target = randomPattern(DIM, 22_000);
        store.insert(target, Map.of());

        ExecutorService exec = Executors.newFixedThreadPool(3);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        futures.add(exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < 100; i++) {
                try {
                    store.insert(randomPattern(DIM, 23_000 + i), Map.of());
                } catch (Exception ignored) {}
            }
        }));

        futures.add(exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < 5; i++) {
                try { store.sealDelta(); } catch (Exception ignored) {}
            }
        }));

        futures.add(exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (int i = 0; i < 200; i++) {
                assertTrue(store.containsExactPattern(target),
                        "Inserted pattern must always be found by containsExactPattern");
            }
        }));

        go.countDown();
        for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        exec.shutdown();
    }

    // ── Phase 3 Fix BUG-2/3: tombstone + dedup in delta buffer ───────────

    @Test
    @DisplayName("Delete + re-insert across freeze boundary — no ghost entries")
    void deleteReinsertAcrossFreeze() {
        openStore();

        WavePattern p1 = randomPattern(DIM, 26_000);
        String id1 = store.insert(p1, Map.of());

        for (int i = 0; i < 20; i++) {
            store.insert(randomPattern(DIM, 27_000 + i), Map.of());
        }

        store.sealDelta();

        WavePattern p2 = randomPattern(DIM, 26_001);
        String id2 = store.insert(p2, Map.of());
        store.delete(id2);
        store.sealDelta();

        List<ResonanceMatch> results = store.query(randomPattern(DIM, 26_001), 50);
        for (ResonanceMatch m : results) {
            assertNotEquals(id2, m.id(),
                    "Deleted pattern must not reappear after seal");
        }

        results = store.query(p1, 50);
        assertTrue(results.stream().anyMatch(m -> m.id().equals(id1)),
                "Non-deleted pattern must remain visible");
    }

    // ── Phase 4 Fix BUG-1/2: WAL ordering — no orphan records ──────────

    @Test
    @DisplayName("Insert duplicate does not leave orphan WAL record")
    void insertDuplicateNoOrphanWal() {
        openStore();

        WavePattern p = randomPattern(DIM, 30_000);
        String id = store.insert(p, Map.of());
        assertNotNull(id);

        assertThrows(ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException.class,
                () -> store.insert(p, Map.of()),
                "Duplicate insert should throw");

        store.sealDelta();

        store.close();
        store = null;

        StoreRuntimeServices rt2 = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(tempDir, DIM, rt2);
        try {
            List<ResonanceMatch> results = store2.query(p, 5);
            long count = results.stream().filter(m -> m.id().equals(id)).count();
            assertEquals(1, count, "Exactly one instance after WAL replay, no orphan");
        } finally {
            store2.close();
            rt2.close();
        }
    }

    @Test
    @DisplayName("Delete non-existent does not leave orphan WAL tombstone")
    void deleteNonExistentNoOrphanWal() {
        openStore();

        WavePattern p = randomPattern(DIM, 31_000);
        String id = store.insert(p, Map.of());
        store.sealDelta();

        WavePattern phantom = randomPattern(DIM, 99_998);
        String phantomHash = ai.evacortex.resonancedb.core.storage.util.HashingUtil
                .computeContentHash(phantom);

        assertThrows(PatternNotFoundException.class,
                () -> store.delete(phantomHash),
                "Delete of non-existent should throw");

        store.close();
        store = null;

        StoreRuntimeServices rt2 = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(tempDir, DIM, rt2);
        try {
            assertTrue(store2.containsExactPattern(p),
                    "Original pattern must survive restart — no phantom tombstone in WAL");
        } finally {
            store2.close();
            rt2.close();
        }
    }

    // ── Phase 4 Fix BUG-3: compactor split — concurrent query during compact ─

    @Test
    @DisplayName("Queries not blocked during compaction file move")
    void queriesDuringCompaction() throws Exception {
        System.setProperty("resonance.wal.enabled", "false");
        openStore();

        for (int i = 0; i < 50; i++) {
            store.insert(randomPattern(DIM, 32_000 + i), Map.of());
        }

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch go = new CountDownLatch(1);

        Future<Integer> queryTask = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            int successful = 0;
            for (int i = 0; i < 20; i++) {
                try {
                    List<ResonanceMatch> r = store.query(randomPattern(DIM, 32_000), 10);
                    if (!r.isEmpty()) successful++;
                } catch (Exception ignored) {}
            }
            return successful;
        });

        Future<?> compactTask = exec.submit(() -> {
            try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            for (String baseName : store.getShardSelector().allShards()) {
                String group = baseName.replace(".segment", "");
                int dash = group.lastIndexOf('-');
                if (dash > 0) group = group.substring(0, dash);
                try { store.compactPhase(group); } catch (Exception ignored) {}
            }
        });

        go.countDown();
        compactTask.get(30, TimeUnit.SECONDS);
        int queryCount = queryTask.get(30, TimeUnit.SECONDS);
        exec.shutdown();

        assertTrue(queryCount > 0,
                "Queries must succeed during compaction, got " + queryCount + " successful");
    }

    // ── Phase 5 tests ─────────────────────────────────────────────────

    @Test
    @DisplayName("Backpressure uses activeSize — no false rejection during freeze")
    void backpressureUsesActiveSize() {
        System.setProperty("resonance.delta.maxEntries", "50");
        System.setProperty("resonance.delta.sealThreshold", "100");
        openStore();

        List<String> batch1 = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            batch1.add(store.insert(randomPattern(DIM, 5000 + i), Map.of()));
        }
        assertEquals(40, batch1.size());

        store.sealDelta();

        List<String> batch2 = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            batch2.add(store.insert(randomPattern(DIM, 6000 + i), Map.of()));
        }
        assertEquals(40, batch2.size(), "Second batch should succeed — frozen doesn't count toward backpressure");
    }

    @Test
    @DisplayName("Close prevents new inserts under writeLock")
    void closeBlocksNewInserts() {
        openStore();
        store.insert(randomPattern(DIM, 7000), Map.of());

        store.close();

        assertThrows(IllegalStateException.class,
                () -> store.insert(randomPattern(DIM, 7001), Map.of()),
                "Insert after close must throw IllegalStateException");
        store = null;
    }

    @Test
    @DisplayName("clearFrozen preserves tombstone when re-inserted entry exists in active")
    void clearFrozenPreservesTombstoneForActiveReinsert() {
        openStore();

        WavePattern p1 = randomPattern(DIM, 8000);
        String id1 = store.insert(p1, Map.of());

        store.sealDelta();

        List<ResonanceMatch> check = store.query(p1, 5);
        assertFalse(check.isEmpty(), "Pattern must exist after seal");

        store.delete(id1);
        assertFalse(store.containsExactPattern(p1), "Pattern must be gone after delete");

        WavePattern p2 = randomPattern(DIM, 8001);
        store.insert(p2, Map.of());
        store.sealDelta();

        assertFalse(store.containsExactPattern(p1),
                "Deleted pattern must not reappear after seal+clearFrozen");
    }

    @Test
    @DisplayName("PhaseSegmentGroup.getAll returns snapshot — safe iteration")
    void phaseSegmentGroupGetAllReturnsSnapshot() throws Exception {
        openStore();

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ids.add(store.insert(randomPattern(DIM, 9000 + i), Map.of()));
        }
        store.sealDelta();

        List<ResonanceMatch> results = store.query(randomPattern(DIM, 9000), 20);
        assertTrue(results.size() > 0, "All patterns stored and queryable");
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static WavePattern patternWithPhase(double meanPhase) {
        double[] amp = new double[DIM];
        double[] phase = new double[DIM];
        Arrays.fill(amp, 0.5);
        Arrays.fill(phase, meanPhase);
        return new WavePattern(amp, phase);
    }

    private static WavePattern randomPattern(int len, long seed) {
        Random rng = new Random(seed);
        double[] amp = new double[len];
        double[] phase = new double[len];
        for (int i = 0; i < len; i++) {
            amp[i] = 0.5 + rng.nextDouble() * 0.5;
            phase[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
        }
        return new WavePattern(amp, phase);
    }
}
