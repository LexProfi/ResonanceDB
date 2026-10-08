/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.storage.responce.InterferenceEntry;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the WAL + DeltaBuffer + Seal pipeline.
 * Verifies end-to-end: insert via delta → query before seal → seal → query after seal.
 */
class DeltaSealIntegrationTest {

    private static final int PATTERN_LEN = 64;

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
    private final Map<String, String> savedProps = new java.util.HashMap<>();

    @BeforeEach
    void setUp() {
        for (String key : MANAGED_PROPS) {
            savedProps.put(key, System.getProperty(key));
        }

        System.setProperty("resonance.wal.enabled", "true");
        System.setProperty("resonance.wal.durability", "strict");
        System.setProperty("resonance.delta.sealThreshold", "100");
        System.setProperty("resonance.delta.maxEntries", "500");
        System.setProperty("resonance.index.enabled", "false");

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

    @Test
    void insertQueryBeforeAndAfterSeal() {
        WavePattern p1 = randomPattern(PATTERN_LEN, 1);
        WavePattern p2 = randomPattern(PATTERN_LEN, 2);

        String id1 = store.insert(p1, Map.of("key", "val1"));
        String id2 = store.insert(p2, Map.of("key", "val2"));

        List<ResonanceMatch> matches = store.query(p1, 5);
        assertFalse(matches.isEmpty(), "Query should find patterns in delta buffer");
        assertTrue(matches.stream().anyMatch(m -> m.id().equals(id1)),
                "Self-match should be found in delta");

        store.sealDelta();

        List<ResonanceMatch> matchesAfter = store.query(p1, 5);
        assertFalse(matchesAfter.isEmpty(), "Query should find patterns after seal");
        assertTrue(matchesAfter.stream().anyMatch(m -> m.id().equals(id1)),
                "Self-match should be found after seal");
    }

    @Test
    void queryDetailedFromDelta() {
        WavePattern p = randomPattern(PATTERN_LEN, 42);
        String id = store.insert(p, Map.of());

        List<ResonanceMatchDetailed> results = store.queryDetailed(p, 5);
        assertFalse(results.isEmpty());
        assertTrue(results.stream().anyMatch(m -> m.id().equals(id)));
        assertTrue(results.getFirst().energy() > 0.9f);
    }

    @Test
    void interferenceMapFromDelta() {
        WavePattern p = randomPattern(PATTERN_LEN, 42);
        String id = store.insert(p, Map.of());

        List<InterferenceEntry> entries = store.queryInterferenceMap(p, 5);
        assertFalse(entries.isEmpty(), "InterferenceMap should include delta patterns");
        assertTrue(entries.stream().anyMatch(e -> e.id().equals(id)));
    }

    @Test
    void deleteFromDelta() {
        WavePattern p = randomPattern(PATTERN_LEN, 1);
        String id = store.insert(p, Map.of());

        assertTrue(store.containsExactPattern(p));

        store.delete(id);

        List<ResonanceMatch> matches = store.query(p, 5);
        assertTrue(matches.stream().noneMatch(m -> m.id().equals(id)),
                "Deleted delta pattern should not appear in query results");
    }

    @Test
    void deleteAfterSeal_noResurrection() {
        WavePattern p = randomPattern(PATTERN_LEN, 1);
        String id = store.insert(p, Map.of());

        store.sealDelta();

        store.delete(id);

        List<ResonanceMatch> matches = store.query(p, 5);
        assertTrue(matches.stream().noneMatch(m -> m.id().equals(id)),
                "Deleted sealed pattern must not appear (A2)");
    }

    @Test
    void bulkInsertWithAutoSeal() {
        int count = 250;
        Set<String> insertedIds = new java.util.HashSet<>();

        for (int i = 0; i < count; i++) {
            WavePattern p = randomPattern(PATTERN_LEN, i);
            String id = store.insert(p, Map.of());
            insertedIds.add(id);
        }

        store.sealDelta();

        WavePattern query = randomPattern(PATTERN_LEN, 0);
        List<ResonanceMatch> matches = store.query(query, count);

        assertFalse(matches.isEmpty());
    }

    @Test
    void closeAndReopenPreservesData() {
        WavePattern p1 = randomPattern(PATTERN_LEN, 1);
        String id1 = store.insert(p1, Map.of("k", "v"));

        store.close();
        store = null;

        StoreRuntimeServices rt2 = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(tempDir, PATTERN_LEN, rt2);
        try {
            List<ResonanceMatch> matches = store2.query(p1, 5);
            assertFalse(matches.isEmpty(), "Pattern should survive close+reopen");
            assertTrue(matches.stream().anyMatch(m -> m.id().equals(id1)));
        } finally {
            store2.close();
            rt2.close();
        }
    }

    @Test
    void walReplayRecoversDeltaAfterCrash() {
        WavePattern p1 = randomPattern(PATTERN_LEN, 1);
        WavePattern p2 = randomPattern(PATTERN_LEN, 2);

        String id1 = store.insert(p1, Map.of());
        String id2 = store.insert(p2, Map.of());

        store.sealDelta();
        WavePattern p3 = randomPattern(PATTERN_LEN, 3);
        String id3 = store.insert(p3, Map.of());

        store.close();
        store = null;

        StoreRuntimeServices rt2 = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(tempDir, PATTERN_LEN, rt2);
        try {
            assertTrue(store2.containsExactPattern(p1), "p1 (sealed) should survive");
            assertTrue(store2.containsExactPattern(p3), "p3 (was in delta, drained on close) should survive");
        } finally {
            store2.close();
            rt2.close();
        }
    }

    @Test
    void duplicateInsertRejectedInDelta() {
        WavePattern p = randomPattern(PATTERN_LEN, 42);
        store.insert(p, Map.of());

        assertThrows(Exception.class, () -> store.insert(p, Map.of()),
                "Duplicate insert in delta should be rejected");
    }

    @Test
    void duplicateInsertRejectedAfterSeal() {
        WavePattern p = randomPattern(PATTERN_LEN, 42);
        store.insert(p, Map.of());
        store.sealDelta();

        assertThrows(Exception.class, () -> store.insert(p, Map.of()),
                "Duplicate insert after seal should be rejected");
    }

    @Test
    void replaceInDelta() {
        WavePattern pOld = randomPattern(PATTERN_LEN, 100);
        WavePattern pNew = randomPattern(PATTERN_LEN, 200);

        String oldId = store.insert(pOld, Map.of("v", "1"));

        String newId = store.replace(oldId, pNew, Map.of("v", "2"));
        assertNotEquals(oldId, newId);

        assertFalse(store.containsExactPattern(pOld), "Old pattern should not exist after replace");
        assertTrue(store.containsExactPattern(pNew), "New pattern should exist after replace");

        List<ResonanceMatch> results = store.query(pNew, 5);
        assertTrue(results.stream().anyMatch(m -> m.id().equals(newId)),
                "Replaced pattern should be findable via query");
    }

    @Test
    void replaceFromSealedToNewDelta() {
        WavePattern pOld = randomPattern(PATTERN_LEN, 300);
        WavePattern pNew = randomPattern(PATTERN_LEN, 400);

        String oldId = store.insert(pOld, Map.of());
        store.sealDelta();

        assertTrue(store.containsExactPattern(pOld));

        String newId = store.replace(oldId, pNew, Map.of());

        assertFalse(store.containsExactPattern(pOld), "Sealed old pattern removed after replace");
        assertTrue(store.containsExactPattern(pNew), "New pattern in delta after replace");

        List<ResonanceMatch> results = store.query(pNew, 5);
        assertTrue(results.stream().anyMatch(m -> m.id().equals(newId)));
    }

    @Test
    void replaceSurvivesCloseReopen() {
        WavePattern pOld = randomPattern(PATTERN_LEN, 500);
        WavePattern pNew = randomPattern(PATTERN_LEN, 600);

        String oldId = store.insert(pOld, Map.of());
        String newId = store.replace(oldId, pNew, Map.of());

        store.close();
        store = null;

        StoreRuntimeServices rt2 = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(tempDir, PATTERN_LEN, rt2);
        try {
            assertFalse(store2.containsExactPattern(pOld), "Old pattern should not survive replace+reopen");
            assertTrue(store2.containsExactPattern(pNew), "New pattern should survive replace+reopen");
        } finally {
            store2.close();
            rt2.close();
        }
    }

    @Test
    void deleteAndReinsertSamePattern() {
        WavePattern p = randomPattern(PATTERN_LEN, 700);
        String id1 = store.insert(p, Map.of());

        store.delete(id1);
        assertFalse(store.containsExactPattern(p), "Deleted pattern should not be found");

        String id2 = store.insert(p, Map.of());
        assertEquals(id1, id2, "Content-addressable: same content = same hash");
        assertTrue(store.containsExactPattern(p), "Re-inserted pattern must be visible");

        List<ResonanceMatch> results = store.query(p, 5);
        assertTrue(results.stream().anyMatch(m -> m.id().equals(id2)),
                "Re-inserted pattern must appear in query results");
    }

    @Test
    void timedSealFlushesDataBelowThreshold() throws Exception {
        store.close();
        runtime.close();

        System.setProperty("resonance.delta.sealThreshold", "999999");
        System.setProperty("resonance.delta.sealIntervalSeconds", "2");

        Path timedDir = tempDir.resolve("timed-seal");
        Files.createDirectories(timedDir);
        runtime = StoreRuntimeServices.fromSystemProperties();
        store = new WavePatternStoreImpl(timedDir, PATTERN_LEN, runtime);

        for (int i = 0; i < 5; i++) {
            store.insert(randomPattern(PATTERN_LEN, 1000 + i), Map.of());
        }

        Path segmentsDir = timedDir.resolve("segments");
        long segmentsBefore = countSegmentFiles(segmentsDir);

        long deadline = System.currentTimeMillis() + 5_000;
        long segmentsAfter = 0;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
            segmentsAfter = countSegmentFiles(segmentsDir);
            if (segmentsAfter > segmentsBefore) break;
        }

        assertTrue(segmentsAfter > segmentsBefore,
                "Timed seal should create segment files even below threshold. " +
                        "Before=" + segmentsBefore + ", after=" + segmentsAfter);

        WavePattern query = randomPattern(PATTERN_LEN, 1000);
        List<ResonanceMatch> results = store.query(query, 5);
        assertFalse(results.isEmpty(), "Query should return results after timed seal");
    }

    @Test
    void replaceInDeltaWithSameContentVisibleInQuery() {
        WavePattern p = randomPattern(PATTERN_LEN, 900);
        String id = store.insert(p, Map.of("version", "1"));

        store.replace(id, p, Map.of("version", "2"));

        assertTrue(store.containsExactPattern(p),
                "Pattern must be visible after replace with same content");

        List<ResonanceMatch> results = store.query(p, 5);
        assertTrue(results.stream().anyMatch(m -> m.id().equals(id)),
                "Replaced pattern must appear in query results");
    }

    @Test
    void allInsertedPatternsVisibleBeforeSeal() {
        int count = 50;
        Set<String> insertedIds = new HashSet<>();
        List<WavePattern> patterns = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            WavePattern p = randomPattern(PATTERN_LEN, 2000 + i);
            String id = store.insert(p, Map.of());
            insertedIds.add(id);
            patterns.add(p);
        }

        WavePattern query = patterns.get(0);
        List<ResonanceMatch> results = store.query(query, count);

        Set<String> returnedIds = new HashSet<>();
        for (ResonanceMatch m : results) returnedIds.add(m.id());

        assertEquals(count, returnedIds.size(),
                "All " + count + " patterns must be returned when topK >= count. " +
                        "Returned " + returnedIds.size());
        assertEquals(insertedIds, returnedIds,
                "Returned IDs must exactly match inserted IDs");
    }

    private static long countSegmentFiles(Path dir) {
        if (!Files.exists(dir)) return 0;
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(".segment")).count();
        } catch (Exception e) {
            return 0;
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
