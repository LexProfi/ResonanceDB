/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.DeltaBuffer.ScoredMatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class DeltaBufferTest {

    private static final int PATTERN_LEN = 64;
    private DeltaBuffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new DeltaBuffer(PATTERN_LEN);
    }

    // ─── Basic CRUD ─────────────────────────────────────────────────────

    @Test
    void addAndContains() {
        WavePattern p = randomPattern(PATTERN_LEN, 1);
        buffer.add("id1", p, Map.of("k", "v"), 0.5, new byte[16], 1L);

        assertTrue(buffer.contains("id1"));
        assertFalse(buffer.contains("id2"));
        assertEquals(1, buffer.size());
    }

    @Test
    void removeFromActive() {
        WavePattern p = randomPattern(PATTERN_LEN, 1);
        buffer.add("id1", p, Map.of(), 0.0, new byte[16], 1L);

        DeltaBuffer.Entry removed = buffer.remove("id1", 2L);
        assertNotNull(removed);
        assertEquals("id1", removed.id());
        assertFalse(buffer.contains("id1"));
        assertEquals(0, buffer.size());
    }

    @Test
    void removeNonExistent() {
        DeltaBuffer.Entry removed = buffer.remove("ghost", 1L);
        assertNull(removed);
    }

    @Test
    void getEntry() {
        WavePattern p = randomPattern(PATTERN_LEN, 42);
        buffer.add("id1", p, Map.of("a", "b"), 1.0, new byte[16], 5L);

        DeltaBuffer.Entry e = buffer.get("id1");
        assertNotNull(e);
        assertEquals("id1", e.id());
        assertEquals(5L, e.lsn());
        assertEquals("b", e.metadata().get("a"));
        assertArrayEquals(p.amplitude(), e.pattern().amplitude());
    }

    // ─── Tombstone semantics ────────────────────────────────────────────

    @Test
    void tombstoneHidesEntry() {
        buffer.add("id1", randomPattern(PATTERN_LEN, 1), Map.of(), 0.0, new byte[16], 1L);
        buffer.markDeleted("id1", 2L);

        assertFalse(buffer.contains("id1"));
        assertNull(buffer.get("id1"));
        // Size includes active entries minus tombstoned — but active was removed by markDeleted
        assertEquals(0, buffer.activeSize());
    }

    @Test
    void tombstoneOnFrozenEntry() {
        buffer.add("id1", randomPattern(PATTERN_LEN, 1), Map.of(), 0.0, new byte[16], 1L);
        buffer.freeze();

        // id1 is now in frozen, remove should create tombstone
        DeltaBuffer.Entry removed = buffer.remove("id1", 2L);
        assertNotNull(removed);
        assertFalse(buffer.contains("id1"));
        assertNull(buffer.get("id1"));
    }

    // ─── Freeze / Clear lifecycle ───────────────────────────────────────

    @Test
    void freezeMovesActiveToFrozen() {
        buffer.add("id1", randomPattern(PATTERN_LEN, 1), Map.of(), 0.0, new byte[16], 1L);
        buffer.add("id2", randomPattern(PATTERN_LEN, 2), Map.of(), 0.0, new byte[16], 2L);

        Map<String, DeltaBuffer.Entry> frozenEntries = buffer.freeze();
        assertEquals(2, frozenEntries.size());
        assertTrue(frozenEntries.containsKey("id1"));
        assertTrue(frozenEntries.containsKey("id2"));

        // Active should be empty
        assertEquals(0, buffer.activeSize());

        // But contains should still see frozen entries
        assertTrue(buffer.contains("id1"));
        assertTrue(buffer.contains("id2"));

        // Total size = frozen entries
        assertEquals(2, buffer.size());
    }

    @Test
    void newInsertsGoToNewActiveAfterFreeze() {
        buffer.add("id1", randomPattern(PATTERN_LEN, 1), Map.of(), 0.0, new byte[16], 1L);
        buffer.freeze();

        buffer.add("id2", randomPattern(PATTERN_LEN, 2), Map.of(), 0.0, new byte[16], 2L);

        assertEquals(1, buffer.activeSize());
        assertEquals(2, buffer.size()); // 1 frozen + 1 active
        assertTrue(buffer.contains("id1")); // in frozen
        assertTrue(buffer.contains("id2")); // in active
    }

    @Test
    void clearFrozenRemovesFrozenEntries() {
        buffer.add("id1", randomPattern(PATTERN_LEN, 1), Map.of(), 0.0, new byte[16], 1L);
        buffer.freeze();

        // Add new entry to active
        buffer.add("id2", randomPattern(PATTERN_LEN, 2), Map.of(), 0.0, new byte[16], 2L);

        buffer.clearFrozen();

        assertFalse(buffer.contains("id1")); // was in frozen, now cleared
        assertTrue(buffer.contains("id2"));  // still in active
        assertEquals(1, buffer.size());
    }

    @Test
    void clearFrozenRemovesTombstonesForFrozenEntries() {
        buffer.add("id1", randomPattern(PATTERN_LEN, 1), Map.of(), 0.0, new byte[16], 1L);
        buffer.freeze();

        // Tombstone id1 while it's frozen
        buffer.markDeleted("id1", 2L);
        assertTrue(buffer.tombstones().containsKey("id1"));

        // After clear, tombstone for id1 should also be removed
        buffer.clearFrozen();
        assertFalse(buffer.tombstones().containsKey("id1"));
    }

    // ─── Scoring ────────────────────────────────────────────────────────

    @Test
    void scoreDeltaFindsSelfMatch() {
        ResonanceKernel kernel = new JavaKernel();
        WavePattern p = randomPattern(PATTERN_LEN, 42);
        String id = "self-match";

        buffer.add(id, p, Map.of(), 0.0, new byte[16], 1L);

        List<ScoredMatch> results = buffer.scoreDelta(p, id, kernel, 5);
        assertFalse(results.isEmpty());

        // Self-match should have high energy
        ScoredMatch best = results.stream()
                .max(Comparator.comparingDouble(sm -> sm.match().energy()))
                .orElseThrow();
        assertEquals(id, best.match().id());
        assertTrue(best.match().energy() > 0.99f,
                "Self-match energy should be ~1.0, got " + best.match().energy());
    }

    @Test
    void scoreDeltaExcludesTombstoned() {
        ResonanceKernel kernel = new JavaKernel();
        WavePattern p = randomPattern(PATTERN_LEN, 42);

        buffer.add("id1", p, Map.of(), 0.0, new byte[16], 1L);
        buffer.markDeleted("id1", 2L);

        List<ScoredMatch> results = buffer.scoreDelta(p, "query", kernel, 5);
        assertTrue(results.isEmpty(), "Tombstoned entries should be excluded from scoring");
    }

    @Test
    void scoreDeltaIncludesFrozenEntries() {
        ResonanceKernel kernel = new JavaKernel();
        WavePattern p1 = randomPattern(PATTERN_LEN, 1);
        WavePattern p2 = randomPattern(PATTERN_LEN, 2);

        buffer.add("id1", p1, Map.of(), 0.0, new byte[16], 1L);
        buffer.freeze();
        buffer.add("id2", p2, Map.of(), 0.0, new byte[16], 2L);

        // Query with p1 — should find both id1 (frozen) and id2 (active)
        List<ScoredMatch> results = buffer.scoreDelta(p1, "query", kernel, 10);

        Set<String> foundIds = new HashSet<>();
        for (ScoredMatch sm : results) foundIds.add(sm.match().id());

        assertTrue(foundIds.contains("id1"), "Frozen entry should be scored");
        assertTrue(foundIds.contains("id2"), "Active entry should be scored");
    }

    @Test
    void scoreDeltaEmptyBufferReturnsEmpty() {
        ResonanceKernel kernel = new JavaKernel();
        WavePattern query = randomPattern(PATTERN_LEN, 1);

        List<ScoredMatch> results = buffer.scoreDelta(query, "q", kernel, 5);
        assertTrue(results.isEmpty());
    }

    // ─── Concurrency ────────────────────────────────────────────────────

    @Test
    void concurrentAddAndScore() throws Exception {
        ResonanceKernel kernel = new JavaKernel();
        int writerCount = 4;
        int insertsPerWriter = 200;
        int readerCount = 4;
        int queriesPerReader = 100;

        ExecutorService pool = Executors.newFixedThreadPool(writerCount + readerCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        // Writers
        for (int w = 0; w < writerCount; w++) {
            int writer = w;
            futures.add(pool.submit(() -> {
                try { start.await(); } catch (InterruptedException e) { return; }
                for (int i = 0; i < insertsPerWriter; i++) {
                    String id = "w" + writer + "-" + i;
                    WavePattern p = randomPattern(PATTERN_LEN, writer * 10000 + i);
                    buffer.add(id, p, Map.of(), 0.0, new byte[16], writer * 10000L + i);
                }
            }));
        }

        // Readers
        for (int r = 0; r < readerCount; r++) {
            int reader = r;
            futures.add(pool.submit(() -> {
                try { start.await(); } catch (InterruptedException e) { return; }
                WavePattern query = randomPattern(PATTERN_LEN, reader + 9999);
                for (int i = 0; i < queriesPerReader; i++) {
                    // Should not throw
                    buffer.scoreDelta(query, "q", kernel, 5);
                }
            }));
        }

        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // All inserts should be visible
        assertEquals(writerCount * insertsPerWriter, buffer.activeSize());
    }

    @Test
    void tombstoneClearedOnAddWithSameLsn() {
        WavePattern p = randomPattern(PATTERN_LEN, 99);
        String id = "replace-self";

        buffer.add(id, p, Map.of(), 0.0, new byte[16], 1L);
        buffer.freeze();

        buffer.remove(id, 5L);
        assertFalse(buffer.contains(id), "After remove, entry should not be visible");

        buffer.add(id, p, Map.of(), 0.0, new byte[16], 5L);
        assertTrue(buffer.contains(id),
                "After add with same LSN as tombstone, entry must be visible");

        ResonanceKernel kernel = new JavaKernel();
        List<ScoredMatch> results = buffer.scoreDelta(p, "q", kernel, 5);
        assertTrue(results.stream().anyMatch(sm -> sm.match().id().equals(id)),
                "Entry must appear in scoreDelta results after same-LSN re-add");
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private static WavePattern randomPattern(int len, long seed) {
        Random rng = new Random(seed);
        double[] amp = new double[len];
        double[] phase = new double[len];
        for (int i = 0; i < len; i++) {
            amp[i] = 0.5 + rng.nextDouble() * 0.5; // positive amplitudes
            phase[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
        }
        return new WavePattern(amp, phase);
    }
}
