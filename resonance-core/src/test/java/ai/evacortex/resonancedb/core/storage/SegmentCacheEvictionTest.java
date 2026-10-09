/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.storage.io.CachedReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentCache;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Determinism, lifetime, concurrency, and cache-pressure tests for SegmentCache.
 *
 * Covers the fix for the cache eviction bug (getIfPresent → getOrLoad),
 * verifies reader pinning via acquire/release, proves no use-after-unmap,
 * and ensures cache state never affects logical visibility.
 */
class SegmentCacheEvictionTest {

    private static final int PATTERN_LEN = 64;
    private static final int N = 20;

    private static final String[] MANAGED_PROPS = {
            "resonance.wal.enabled", "resonance.wal.durability",
            "resonance.delta.sealThreshold", "resonance.delta.maxEntries",
            "resonance.delta.sealIntervalSeconds",
            "resonance.index.enabled", "resonance.cache.maxBytes"
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
        System.setProperty("resonance.delta.sealThreshold", "5");
        System.setProperty("resonance.delta.maxEntries", "500");
        System.setProperty("resonance.delta.sealIntervalSeconds", "9999");
        System.setProperty("resonance.index.enabled", "false");
        System.clearProperty("resonance.cache.maxBytes");

        runtime = StoreRuntimeServices.fromSystemProperties();
        store = new WavePatternStoreImpl(tempDir, PATTERN_LEN, runtime);
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

    // ═══════════════════════════════════════════════════════════════════
    // Section A-G: Deterministic correctness tests
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void testA_selfMatchAfterSeal() {
        List<String> ids = insertAndSeal(N, 1000);
        for (int i = 0; i < N; i++) {
            WavePattern query = randomPattern(PATTERN_LEN, 1000 + i);
            List<ResonanceMatch> results = store.query(query, 1);
            assertFalse(results.isEmpty(), "Pattern " + i + " must be found");
            assertEquals(ids.get(i), results.get(0).id(),
                    "R@1 must be self-match for pattern " + i);
        }
    }

    @Test
    void testB_querySurvivesCacheEviction() throws Exception {
        List<String> ids = insertAndSeal(N, 2000);
        WavePattern query = randomPattern(PATTERN_LEN, 2000);
        List<ResonanceMatch> baseline = store.query(query, N);
        assertFalse(baseline.isEmpty());
        int baselineCount = baseline.size();

        evictAllCachedReaders();

        List<ResonanceMatch> afterEviction = store.query(query, N);
        assertEquals(baselineCount, afterEviction.size(),
                "Query count must be identical after eviction");
        assertTrue(afterEviction.stream().anyMatch(m -> m.id().equals(ids.get(0))),
                "Self-match must survive eviction");
    }

    @Test
    void testB2_materializePatternsSurvivesEviction() throws Exception {
        WavePattern p = randomPattern(PATTERN_LEN, 8000);
        String id = store.insert(p, Map.of());
        store.sealDelta();

        List<ResonanceMatch> baseline = store.query(p, 5);
        ResonanceMatch baselineMatch = baseline.stream()
                .filter(m -> m.id().equals(id)).findFirst().orElseThrow();
        assertNotNull(baselineMatch.pattern());

        evictAllCachedReaders();

        List<ResonanceMatch> afterEviction = store.query(p, 5);
        assertFalse(afterEviction.isEmpty());
        ResonanceMatch match = afterEviction.stream()
                .filter(m -> m.id().equals(id)).findFirst().orElseThrow();
        assertNotNull(match.pattern(), "Pattern data must survive eviction");
    }

    @Test
    void testC_querySurvivesCloseReopen() {
        List<String> ids = insertAndSeal(N, 3000);
        store.close();
        store = null;

        StoreRuntimeServices rt2 = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(tempDir, PATTERN_LEN, rt2);
        try {
            for (int i = 0; i < N; i++) {
                WavePattern query = randomPattern(PATTERN_LEN, 3000 + i);
                List<ResonanceMatch> results = store2.query(query, 1);
                assertFalse(results.isEmpty(), "Pattern " + i + " must survive reopen");
                assertEquals(ids.get(i), results.get(0).id());
            }
        } finally {
            store2.close();
            rt2.close();
        }
    }

    @Test
    void testD_deleteRemovesFromQuery() {
        WavePattern p = randomPattern(PATTERN_LEN, 4000);
        String id = store.insert(p, Map.of());
        store.sealDelta();
        assertTrue(store.query(p, 5).stream().anyMatch(m -> m.id().equals(id)));
        store.delete(id);
        assertTrue(store.query(p, 5).stream().noneMatch(m -> m.id().equals(id)),
                "Deleted pattern must not appear");
    }

    @Test
    void testE_replaceSwapsPattern() {
        WavePattern pOld = randomPattern(PATTERN_LEN, 5000);
        WavePattern pNew = randomPattern(PATTERN_LEN, 5001);
        String oldId = store.insert(pOld, Map.of());
        store.sealDelta();
        String newId = store.replace(oldId, pNew, Map.of());
        store.sealDelta();
        List<ResonanceMatch> results = store.query(pNew, 5);
        assertTrue(results.stream().anyMatch(m -> m.id().equals(newId)));
        assertTrue(results.stream().noneMatch(m -> m.id().equals(oldId)));
    }

    @Test
    void testF_querySurvivesCompaction() {
        List<String> ids = new ArrayList<>();
        List<WavePattern> patterns = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            WavePattern p = randomPattern(PATTERN_LEN, 6000 + i);
            ids.add(store.insert(p, Map.of()));
            patterns.add(p);
            if ((i + 1) % 5 == 0) store.sealDelta();
        }
        store.sealDelta();

        for (int i = 0; i < 21; i++) store.delete(ids.get(i));
        store.sealDelta();

        Set<String> groups = getSegmentGroupKeys();
        for (String group : groups) store.compactPhase(group);

        for (int i = 21; i < 30; i++) {
            WavePattern query = patterns.get(i);
            List<ResonanceMatch> results = store.query(query, 1);
            assertFalse(results.isEmpty(), "Surviving pattern " + i + " found after compaction");
            assertEquals(ids.get(i), results.get(0).id());
        }
    }

    @Test
    void testG_repeatedQueryAfterEvictionIsDeterministic() throws Exception {
        insertAndSeal(N, 7000);
        WavePattern query = randomPattern(PATTERN_LEN, 7000);

        List<String> refIds = null;
        for (int round = 0; round < 100; round++) {
            evictAllCachedReaders();
            List<ResonanceMatch> current = store.query(query, N);
            List<String> curIds = current.stream().map(ResonanceMatch::id).toList();
            if (refIds == null) {
                refIds = curIds;
                assertFalse(refIds.isEmpty());
            } else {
                assertEquals(refIds, curIds, "Round " + round + ": results must be identical");
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Section 2: Eviction race — reader pinned during active use
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void testEvictionRace_readerPinnedDuringActiveUse() throws Exception {
        WavePattern p = randomPattern(PATTERN_LEN, 9000);
        store.insert(p, Map.of());
        store.sealDelta();

        SegmentCache segmentCache = getReaderCache();
        String segName = getAnySegmentName();

        CachedReader reader = segmentCache.getOrLoad(segName);
        assertNotNull(reader);
        reader.acquire();

        // Reader is pinned (refCount=1). Now simulate eviction.
        evictAllCachedReaders();

        // Reader should still be usable — mmap NOT unmapped because refCount > 0
        Set<String> ids = reader.allIds();
        assertFalse(ids.isEmpty(), "Pinned reader must still be readable after eviction");

        // Verify we can read actual pattern data from mmap
        for (String id : ids) {
            WavePattern read = reader.readById(id);
            assertNotNull(read, "Pinned reader must return pattern data after eviction");
            assertEquals(PATTERN_LEN, read.amplitude().length);
            break;
        }

        reader.release();
        // After release, reader is eligible for unmap (closed + refCount=0)
    }

    @Test
    void testEvictionRace_deterministicInterleaving() throws Exception {
        WavePattern p = randomPattern(PATTERN_LEN, 9100);
        store.insert(p, Map.of());
        store.sealDelta();

        SegmentCache segmentCache = getReaderCache();
        String segName = getAnySegmentName();

        CountDownLatch readerAcquired = new CountDownLatch(1);
        CountDownLatch evictionDone = new CountDownLatch(1);
        CountDownLatch readComplete = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        // Thread 1: acquire reader, signal, wait for eviction, read, release
        Thread t1 = new Thread(() -> {
            try {
                CachedReader reader = segmentCache.getOrLoad(segName);
                assertNotNull(reader);
                reader.acquire();
                readerAcquired.countDown();

                // Wait for T2 to evict
                assertTrue(evictionDone.await(10, TimeUnit.SECONDS));

                // Reader still usable — mmap pinned by refCount
                Set<String> ids = reader.allIds();
                assertFalse(ids.isEmpty());
                for (String id : ids) {
                    WavePattern read = reader.readById(id);
                    assertNotNull(read);
                    break;
                }

                readComplete.countDown();
                reader.release();
            } catch (Throwable t) {
                error.set(t);
                readerAcquired.countDown();
                readComplete.countDown();
            }
        });

        // Thread 2: wait for acquire, then evict
        Thread t2 = new Thread(() -> {
            try {
                assertTrue(readerAcquired.await(10, TimeUnit.SECONDS));
                evictAllCachedReaders();
                evictionDone.countDown();
            } catch (Throwable t) {
                error.set(t);
                evictionDone.countDown();
            }
        });

        t1.start();
        t2.start();
        t1.join(15_000);
        t2.join(15_000);

        assertNull(error.get(), () -> "Race test failed: " + error.get());
        assertTrue(readComplete.await(1, TimeUnit.SECONDS), "Read must complete");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Section 3: Acquire-after-close race
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void testAcquireAfterClose_throwsISE_callerHandles() throws Exception {
        WavePattern p = randomPattern(PATTERN_LEN, 9200);
        store.insert(p, Map.of());
        store.sealDelta();

        SegmentCache segmentCache = getReaderCache();
        String segName = getAnySegmentName();

        CachedReader reader = segmentCache.getOrLoad(segName);
        assertNotNull(reader);

        // Evict → close the reader
        evictAllCachedReaders();

        // acquire() on closed reader must throw ISE
        assertThrows(IllegalStateException.class, reader::acquire,
                "acquire() on closed reader must throw ISE");

        // But store.query() must still work — it gets a NEW reader via getOrLoad()
        List<ResonanceMatch> results = store.query(p, 5);
        assertFalse(results.isEmpty(), "Query must find patterns via reloaded reader");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Section 4: Version lifecycle
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void testVersionLifecycle_appendFlush() throws Exception {
        WavePattern p1 = randomPattern(PATTERN_LEN, 9300);
        store.insert(p1, Map.of());
        store.sealDelta();

        SegmentCache cache = getReaderCache();
        long loadsBefore = cache.getLoadCount();

        // Insert more patterns → new version
        WavePattern p2 = randomPattern(PATTERN_LEN, 9301);
        store.insert(p2, Map.of());
        store.sealDelta();

        // Query should find BOTH patterns — new version reader loaded
        List<ResonanceMatch> r1 = store.query(p1, 5);
        List<ResonanceMatch> r2 = store.query(p2, 5);
        assertFalse(r1.isEmpty(), "p1 found after append");
        assertFalse(r2.isEmpty(), "p2 found after append");
    }

    @Test
    void testVersionLifecycle_replaceNoResurrection() throws Exception {
        WavePattern pOld = randomPattern(PATTERN_LEN, 9400);
        WavePattern pNew = randomPattern(PATTERN_LEN, 9401);
        String oldId = store.insert(pOld, Map.of());
        store.sealDelta();

        // Evict to ensure we go through full cache lifecycle
        evictAllCachedReaders();

        String newId = store.replace(oldId, pNew, Map.of());
        store.sealDelta();

        evictAllCachedReaders();

        // New pattern visible, old gone — no resurrection from stale mmap
        assertTrue(store.query(pNew, 5).stream().anyMatch(m -> m.id().equals(newId)));
        assertTrue(store.query(pOld, 5).stream().noneMatch(m -> m.id().equals(oldId)));
    }

    @Test
    void testVersionLifecycle_compactionWithActiveReader() throws Exception {
        // Create enough patterns for compaction
        List<String> ids = new ArrayList<>();
        List<WavePattern> patterns = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            WavePattern p = randomPattern(PATTERN_LEN, 9500 + i);
            ids.add(store.insert(p, Map.of()));
            patterns.add(p);
            if ((i + 1) % 5 == 0) store.sealDelta();
        }
        store.sealDelta();

        // Pin a reader to old segment
        SegmentCache segmentCache = getReaderCache();
        String segName = getAnySegmentName();
        CachedReader pinnedReader = segmentCache.getOrLoad(segName);
        assertNotNull(pinnedReader);
        pinnedReader.acquire();

        // Delete to enable compaction
        for (int i = 0; i < 18; i++) store.delete(ids.get(i));
        store.sealDelta();

        // Trigger compaction — old segments merged
        Set<String> groups = getSegmentGroupKeys();
        for (String group : groups) store.compactPhase(group);

        // Verify surviving patterns still queryable
        for (int i = 18; i < 25; i++) {
            List<ResonanceMatch> results = store.query(patterns.get(i), 1);
            assertFalse(results.isEmpty(), "Pattern " + i + " must survive compaction");
        }

        // Pinned reader still readable (mmap alive)
        Set<String> pinnedIds = pinnedReader.allIds();
        assertNotNull(pinnedIds);

        pinnedReader.release();
    }

    // ═══════════════════════════════════════════════════════════════════
    // Section 9: Determinism under eviction — extended with scores
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void testDeterminism_evictAndQueryWithScores() throws Exception {
        insertAndSeal(N, 7500);
        WavePattern query = randomPattern(PATTERN_LEN, 7500);

        List<String> refIds = null;
        List<Float> refScores = null;

        for (int round = 0; round < 100; round++) {
            evictAllCachedReaders();
            List<ResonanceMatch> current = store.query(query, N);
            List<String> curIds = current.stream().map(ResonanceMatch::id).toList();
            List<Float> curScores = current.stream().map(ResonanceMatch::energy).toList();
            if (refIds == null) {
                refIds = curIds;
                refScores = curScores;
                assertFalse(refIds.isEmpty());
            } else {
                assertEquals(refIds, curIds, "Round " + round + ": IDs must match");
                assertEquals(refScores, curScores, "Round " + round + ": scores must match");
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Section 11: Load failure semantics
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void testLoadFailure_missingSegmentFile() throws Exception {
        WavePattern p = randomPattern(PATTERN_LEN, 9600);
        store.insert(p, Map.of());
        store.sealDelta();

        // Delete the actual segment file on disk
        String segName = getAnySegmentName();
        Path segPath = tempDir.resolve("segments").resolve(segName);
        assertTrue(java.nio.file.Files.exists(segPath), "Segment file must exist before deletion");
        java.nio.file.Files.delete(segPath);

        // Evict to force reload attempt
        evictAllCachedReaders();

        // getOrLoad() should return null (load failure), NOT crash
        SegmentCache cache = getReaderCache();
        CachedReader reader = cache.getOrLoad(segName);
        // Reader should be null because the file doesn't exist
        assertNull(reader, "getOrLoad() must return null for missing segment file");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Concurrent stress tests
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void testH_concurrentInsertQueryWithEviction() throws Exception {
        int nThreads = 4;
        int patternsPerThread = 10;
        CyclicBarrier barrier = new CyclicBarrier(nThreads);
        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(nThreads);

        insertAndSeal(10, 10000);
        WavePattern baseQuery = randomPattern(PATTERN_LEN, 10000);

        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        try {
            for (int t = 0; t < nThreads; t++) {
                final int threadIdx = t;
                pool.submit(() -> {
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                        for (int i = 0; i < patternsPerThread; i++) {
                            WavePattern p = randomPattern(PATTERN_LEN, 20000 + threadIdx * 1000 + i);
                            try { store.insert(p, Map.of()); } catch (Exception ignored) {}
                            if (i % 3 == 0) {
                                List<ResonanceMatch> results = store.query(baseQuery, 5);
                                if (results == null) errors.incrementAndGet();
                            }
                            if (i % 5 == 0) {
                                try { evictAllCachedReaders(); } catch (Exception ignored) {}
                            }
                        }
                    } catch (Exception e) { errors.incrementAndGet(); }
                    finally { done.countDown(); }
                });
            }
            assertTrue(done.await(60, TimeUnit.SECONDS));
            assertEquals(0, errors.get(), "No errors during concurrent test");
        } finally { pool.shutdown(); }
    }

    @Test
    void testI_concurrentEvictionDuringQuery() throws Exception {
        insertAndSeal(N, 11000);
        WavePattern query = randomPattern(PATTERN_LEN, 11000);
        AtomicInteger queryErrors = new AtomicInteger(0);
        AtomicInteger evictErrors = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(2);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < 50; i++) {
                        try { store.query(query, N); } catch (Exception e) { queryErrors.incrementAndGet(); }
                    }
                } finally { done.countDown(); }
            });
            pool.submit(() -> {
                try {
                    for (int i = 0; i < 50; i++) {
                        try { evictAllCachedReaders(); } catch (Exception e) { evictErrors.incrementAndGet(); }
                    }
                } finally { done.countDown(); }
            });
            assertTrue(done.await(60, TimeUnit.SECONDS));
            assertEquals(0, queryErrors.get());
            assertEquals(0, evictErrors.get());
        } finally { pool.shutdown(); }
    }

    @Test
    void testDeterminism_concurrentQueryAfterEviction() throws Exception {
        insertAndSeal(N, 12000);
        WavePattern query = randomPattern(PATTERN_LEN, 12000);

        // Get reference result
        List<String> refIds = store.query(query, N).stream().map(ResonanceMatch::id).toList();
        assertFalse(refIds.isEmpty());

        int nThreads = 4;
        int rounds = 25;
        AtomicInteger mismatches = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(nThreads);
        CyclicBarrier barrier = new CyclicBarrier(nThreads + 1);

        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        try {
            for (int t = 0; t < nThreads; t++) {
                pool.submit(() -> {
                    try {
                        for (int r = 0; r < rounds; r++) {
                            barrier.await(10, TimeUnit.SECONDS);
                            List<String> ids = store.query(query, N).stream()
                                    .map(ResonanceMatch::id).toList();
                            if (!refIds.equals(ids)) mismatches.incrementAndGet();
                        }
                    } catch (Exception e) {
                        mismatches.incrementAndGet();
                    } finally { done.countDown(); }
                });
            }
            // Main thread: evict before each round, then release query threads
            for (int r = 0; r < rounds; r++) {
                evictAllCachedReaders();
                barrier.await(10, TimeUnit.SECONDS);
            }
            assertTrue(done.await(60, TimeUnit.SECONDS));
            assertEquals(0, mismatches.get(), "All concurrent queries must produce identical results");
        } finally { pool.shutdown(); }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    private List<String> insertAndSeal(int count, long seedBase) {
        List<String> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            WavePattern p = randomPattern(PATTERN_LEN, seedBase + i);
            ids.add(store.insert(p, Map.of()));
        }
        store.sealDelta();
        return ids;
    }

    private void evictAllCachedReaders() throws Exception {
        Field readerCacheField = WavePatternStoreImpl.class.getDeclaredField("readerCache");
        readerCacheField.setAccessible(true);
        SegmentCache segmentCache = (SegmentCache) readerCacheField.get(store);
        Field cacheField = SegmentCache.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        LoadingCache<?, ?> caffeineCache = (LoadingCache<?, ?>) cacheField.get(segmentCache);
        caffeineCache.invalidateAll();
        caffeineCache.cleanUp();
    }

    private SegmentCache getReaderCache() throws Exception {
        Field f = WavePatternStoreImpl.class.getDeclaredField("readerCache");
        f.setAccessible(true);
        return (SegmentCache) f.get(store);
    }

    private String getAnySegmentName() {
        try {
            Field f = WavePatternStoreImpl.class.getDeclaredField("manifest");
            f.setAccessible(true);
            ManifestIndex manifest = (ManifestIndex) f.get(store);
            return manifest.getAllSegmentNames().iterator().next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> getSegmentGroupKeys() {
        try {
            Field f = WavePatternStoreImpl.class.getDeclaredField("segmentGroups");
            f.setAccessible(true);
            ConcurrentMap<String, ?> map = (ConcurrentMap<String, ?>) f.get(store);
            return new HashSet<>(map.keySet());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
