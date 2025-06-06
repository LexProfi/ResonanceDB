/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.*;
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.HashingUtil;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WavePatternStoreImplTest {

    private Path tempDir;
    private WavePatternStoreImpl store;

    @BeforeAll
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("resonance-test");
        store = new WavePatternStoreImpl(tempDir);
    }

    @AfterAll
    void cleanup() throws Exception {
        if (store != null) store.close();
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void testInsertAndQuery() throws IOException {
        double[] amp = {0.37454012, 0.9507143, 0.7319939, 0.5986584};
        double[] phase = {0.0, 0.1, 0.2, 0.3};
        WavePattern psi = new WavePattern(amp, phase);

        String insertedId = store.insert(psi, Map.of());
        System.out.println("ID = " + insertedId);
        System.out.println(store.query(psi, 10).size());
        Files.readAllLines(Paths.get(tempDir + "/index/manifest.idx"));
        List<ResonanceMatch> results = store.query(psi, 1);
        assertEquals(1, results.size());

        ResonanceMatch match = results.getFirst();
        assertArrayEquals(amp, match.pattern().amplitude(), 1e-9);
        assertArrayEquals(phase, match.pattern().phase(), 1e-9);

        assertEquals(1.0f, match.energy(), 1e-5);
    }

    @Test
    void testInsertWithIdAndDelete() {
        WavePattern psi = WavePatternTestUtils.createConstantPattern(0.1, 0.1, 4);
        String id = HashingUtil.computeContentHash(psi);
        Map<String, String> meta = Map.of("source", "test");

        store.insert(psi, meta);
        List<ResonanceMatch> results = store.query(psi, 1);

        assertEquals(1, results.size());
        assertEquals(id, results.getFirst().id());

        store.delete(id);
        assertThrows(PatternNotFoundException.class, () -> store.delete(id));
    }

    @Test
    void testUpdate() {
        WavePattern original = WavePatternTestUtils.createConstantPattern(0.3, 0.1, 8);
        String id = HashingUtil.computeContentHash(original);
        store.insert(original, Map.of());
        WavePattern updated = WavePatternTestUtils.createConstantPattern(0.9, 1.2, 8);
        store.update(id, updated, Map.of());
        List<ResonanceMatch> matches = store.query(updated, 1);
        assertFalse(matches.isEmpty(), "updated pattern must be found");
        assertEquals(id, matches.getFirst().id(), "ID should remain unchanged after update");
        assertTrue(matches.getFirst().energy() > 0.95f, "energy with itself must stay high");
        List<ResonanceMatch> oldMatch = store.query(original, 1);
        assertFalse(oldMatch.isEmpty(), "query should still return a match");
        assertEquals(id, oldMatch.getFirst().id(), "same business object, so same ID");
        assertTrue(oldMatch.getFirst().energy() < 0.95f,
                "energy for outdated waveform must drop below threshold");
    }

    @Test
    void testCompareDifferentPhases() {
        WavePattern a = WavePatternTestUtils.createConstantPattern(1.0, 0.0, 16);
        WavePattern b = WavePatternTestUtils.createConstantPattern(1.0, Math.PI, 16);
        float score = store.compare(a, b);
        assertTrue(score < 0.05f);
    }

    @Test
    void testCompareIdentical() {
        WavePattern a = WavePatternTestUtils.createConstantPattern(1.0, 0.0, 16);
        WavePattern b = WavePatternTestUtils.createConstantPattern(1.0, 0.0, 16);
        float score = store.compare(a, b);
        assertEquals(1.0f, score, 1e-5);
    }

    @Test
    void testInsertDuplicateThrows() {
        WavePattern psi = WavePatternTestUtils.createConstantPattern(0.5, 0.5, 8);
        store.insert(psi, Map.of());
        assertThrows(DuplicatePatternException.class, () -> store.insert(psi, Map.of()));
    }

    @Test
    void testDeleteNonExistentThrows() {
        assertThrows(PatternNotFoundException.class, () -> store.delete("0123456789abcdef0123456789abcdef"));
    }

    @Test
    void testEmptyAndEdgeLengthPatterns() {
        WavePattern[] patterns = {
                WavePatternTestUtils.createConstantPattern(0.1, 0.1, 1),
                WavePatternTestUtils.createConstantPattern(1.0, 0.5, 512),
                WavePatternTestUtils.createConstantPattern(1.0, 0.0, 1024)
        };

        for (WavePattern pattern : patterns) {
            String insertedId = store.insert(pattern, Map.of());
            String matchedId = store.query(pattern, 1).getFirst().id();
            assertEquals(insertedId, matchedId);
        }
    }

    @Test
    void testMultipleSegmentsCreated() throws IOException {
        for (int i = 0; i < 20; i++) {
            WavePattern psi = WavePatternTestUtils.createConstantPattern(1.0, i * 0.5, 64);
            store.insert(psi, Map.of("index", String.valueOf(i)));
        }
        long segmentCount = Files.list(tempDir.resolve("segments")).count();
        assertTrue(segmentCount >= 2);
    }

    @Test
    void testMetadataWritten() throws IOException {
        WavePattern psi = WavePatternTestUtils.createConstantPattern(0.6, 0.4, 8);
        Map<String, String> meta = Map.of("type", "test", "source", "unit");

        String id = store.insert(psi, meta);
        String json = Files.readString(tempDir.resolve("metadata/pattern-meta.json"));

        assertTrue(json.contains("\"" + id + "\""), "Meta should contain pattern ID");
        assertTrue(json.contains("\"type\""), "Meta should contain 'type'");
        assertTrue(json.contains("\"unit\""), "Meta should contain 'unit'");
    }

    @Test
    void testInsertWithAutoId() {
        WavePattern psi = WavePatternTestUtils.fixedPattern();
        String id1 = store.insert(psi, Map.of());
        assertNotNull(id1);
        DuplicatePatternException ex = assertThrows(DuplicatePatternException.class, () -> store.insert(psi, Map.of()));
        assertTrue(ex.getMessage().contains(id1));
    }

    @Test
    void testPersistenceAfterReopen() throws Exception {
        Path dir = Files.createTempDirectory("resonance-persist");
        WavePatternStoreImpl first = new WavePatternStoreImpl(dir);

        WavePattern psi = WavePatternTestUtils.createConstantPattern(0.7, 0.3, 16);
        String id = first.insert(psi, Map.of("case", "persist"));
        first.close();

        WavePatternStoreImpl reopened = new WavePatternStoreImpl(dir);
        try {
            List<ResonanceMatch> hits = reopened.query(psi, 1);

            assertFalse(hits.isEmpty(), "pattern must survive reopen");
            assertEquals(id,  hits.getFirst().id(), "ID must match");
            assertEquals(1.0f, hits.getFirst().energy(), 1e-5,
                    "energy with identical ψ must be 1");

            String metaJson = Files.readString(dir.resolve("metadata/pattern-meta.json"));
            assertTrue(metaJson.contains("\"" + id + "\""));
            assertTrue(metaJson.contains("\"case\""));

        } finally {
            reopened.close();
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    void testDeleteRemovesFromQuery() throws Exception {
        Path dir = Files.createTempDirectory("resonance-delete");

        try (WavePatternStoreImpl localStore = new WavePatternStoreImpl(dir)) {
            WavePattern psi = WavePatternTestUtils.createConstantPattern(0.42, 0.13, 32);
            String id = localStore.insert(psi, Map.of());
            assertEquals(id, localStore.query(psi, 1).getFirst().id());
            localStore.delete(id);
            List<ResonanceMatch> hits = localStore.query(psi, 3);
            boolean idStillPresent = hits.stream().anyMatch(m -> m.id().equals(id));
            assertFalse(idStillPresent, "deleted pattern must not appear in subsequent query results");

            if (!hits.isEmpty()) {
                assertTrue(hits.getFirst().energy() < 0.95f,
                        "energy with deleted ψ must drop below similarity threshold");
            }

            assertThrows(PatternNotFoundException.class, () -> localStore.delete(id));

        } finally {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    void testTopKOrderingAndSelfPriority() {
        final int TOTAL = 10;
        final double AMP = 1.0;
        final int LEN = 32;
        String[] ids = new String[TOTAL];
        for (int i = 0; i < TOTAL; i++) {
            double phaseShift = i * 0.2;
            WavePattern p = WavePatternTestUtils
                    .createConstantPattern(AMP, phaseShift, LEN);

            ids[i] = store.insert(p, Map.of("idx", String.valueOf(i)));
        }
        int targetIdx = 3;
        WavePattern query = WavePatternTestUtils
                .createConstantPattern(AMP, targetIdx * 0.2, LEN);
        int TOP_K = 5;
        List<ResonanceMatch> hits = store.query(query, TOP_K);
        assertEquals(TOP_K, hits.size(), "must return exactly top-K results");
        assertEquals(ids[targetIdx], hits.getFirst().id(), "self-match should have highest priority");
        for (int i = 1; i < hits.size(); i++) {
            assertTrue(hits.get(i-1).energy() >= hits.get(i).energy(),
                    "energies must be sorted in descending order");
        }
        assertEquals(1.0f, hits.getFirst().energy(), 1e-5, "self-match energy should be 1");
    }

    @Test
    void testQueryDetailedWithZonesAndPhaseShift() {
        WavePattern core = WavePatternTestUtils.createConstantPattern(1.0, 0.0, 16);       // точно совпадает
        WavePattern fringe = WavePatternTestUtils.createConstantPattern(1.0, 0.5, 16);     // частичное совпадение
        WavePattern shadow = WavePatternTestUtils.createConstantPattern(1.0, Math.PI, 16); // противоположная фаза

        String idCore = store.insert(core, Map.of("label", "core"));
        String idFringe = store.insert(fringe, Map.of("label", "fringe"));
        String idShadow = store.insert(shadow, Map.of("label", "shadow"));

        InterferenceMap map = store.queryInterference(core, 5);
        List<ResonanceMatchDetailed> matches = map.matches();

        assertEquals(core, map.query(), "query pattern must be preserved");

        boolean foundCore = false, foundFringe = false, foundShadow = false;

        for (ResonanceMatchDetailed match : matches) {
            System.out.printf("Match ID: %s | Energy: %.4f | Δφ: %.3f | Zone: %s%n",
                    match.id(), match.energy(), match.phaseDelta(), match.zone());

            if (match.id().equals(idCore)) {
                foundCore = true;
                assertEquals(ResonanceZone.CORE, match.zone(), "Expected CORE match");
                assertEquals(0.0, match.phaseDelta(), 1e-6, "Expected phase shift near zero");
            }

            if (match.id().equals(idFringe)) {
                foundFringe = true;
                assertEquals(ResonanceZone.FRINGE, match.zone(), "Expected FRINGE match");
                assertTrue(match.phaseDelta() > 0.1 && match.phaseDelta() < 1.0,
                        "Expected moderate phase delta");
            }

            if (match.id().equals(idShadow)) {
                foundShadow = true;
                assertEquals(ResonanceZone.SHADOW, match.zone(), "Expected SHADOW match");
                assertTrue(match.phaseDelta() > 2.5, "Expected large phase shift");
            }
        }

        assertTrue(foundCore, "Core match must be present");
        assertTrue(foundFringe, "Fringe match must be present");
        assertTrue(foundShadow, "Shadow match must be present");
    }

    @Test
    void testZoneScoreMonotonicityByZone() {
        WavePattern core = WavePatternTestUtils.createConstantPattern(
                1.0, 0.0 + Math.random() * 1e-4, 32);
        WavePattern fringe = WavePatternTestUtils.createConstantPattern(
                1.0, 0.5 + Math.random() * 1e-4, 32);
        WavePattern shadow = WavePatternTestUtils.createConstantPattern(
                1.0, Math.PI + Math.random() * 1e-4, 32);

        String idCore = store.insert(core, Map.of());
        String idFringe = store.insert(fringe, Map.of());
        String idShadow = store.insert(shadow, Map.of());

        InterferenceMap map = store.queryInterference(core, 5);
        List<ResonanceMatchDetailed> matches = map.matches();

        double scoreCore = -1, scoreFringe = -1, scoreShadow = -1;

        for (ResonanceMatchDetailed m : matches) {
            if (m.id().equals(idCore))   scoreCore = m.zoneScore();
            if (m.id().equals(idFringe)) scoreFringe = m.zoneScore();
            if (m.id().equals(idShadow)) scoreShadow = m.zoneScore();
        }

        System.out.printf("Zone scores: CORE=%.4f | FRINGE=%.4f | SHADOW=%.4f%n", scoreCore, scoreFringe, scoreShadow);

        assertTrue(scoreCore >= 0.9, "core should have very high score");
        assertTrue(scoreFringe > 0.1 && scoreFringe < scoreCore, "fringe should be intermediate");
        assertTrue(scoreShadow < 0.05, "shadow should be near zero");
    }

    @Test
    void testCoreZoneWithIdenticalPattern() throws Exception {
        Path tempDir = Files.createTempDirectory("resonance-test");
        try (WavePatternStoreImpl store = new WavePatternStoreImpl(tempDir)) {
            WavePattern pattern = WavePatternTestUtils.createConstantPattern(1.0, 0.0, 128);
            String id = store.insert(pattern, Map.of("kind", "test"));

            List<InterferenceEntry> results = store.queryInterferenceMap(pattern, 1);
            assertEquals(1, results.size());

            InterferenceEntry entry = results.getFirst();
            assertEquals(id, entry.id());
            assertEquals(ResonanceZone.CORE, entry.zone());
            assertTrue(entry.phaseShift() < 1e-6, "Phase shift should be ≈ 0 for identical pattern");
            assertTrue(entry.energy() > 0.99f);
        } finally {
            TestUtils.deleteDirectoryRecursive(tempDir);
        }
    }

    @Test
    void testFringeZoneWithShiftedPhase() throws Exception {
        Path tempDir = Files.createTempDirectory("resonance-test");
        try (WavePatternStoreImpl store = new WavePatternStoreImpl(tempDir)) {

            WavePattern pattern = WavePatternTestUtils.createConstantPattern(1.0, 0.0, 128);
            WavePattern slightlyShifted = WavePatternTestUtils.createConstantPattern(1.0, 0.399, 128);

            store.insert(pattern, Map.of());
            List<InterferenceEntry> results = store.queryInterferenceMap(slightlyShifted, 1);
            assertEquals(1, results.size());

            InterferenceEntry entry = results.getFirst();
            assertEquals(ResonanceZone.FRINGE, entry.zone());
            assertTrue(entry.phaseShift() > 0.1 && entry.phaseShift() < Math.PI);
        } finally {
            TestUtils.deleteDirectoryRecursive(tempDir);
        }
    }

    @Test
    void testShadowZoneWithOppositePhase() throws Exception {
        Path tempDir = Files.createTempDirectory("resonance-test");
        try (WavePatternStoreImpl store = new WavePatternStoreImpl(tempDir)) {

            WavePattern pattern = WavePatternTestUtils.createConstantPattern(1.0, 0.0, 128);
            WavePattern opposite = WavePatternTestUtils.createConstantPattern(1.0, Math.PI, 128);

            store.insert(pattern, Map.of());
            List<InterferenceEntry> results = store.queryInterferenceMap(opposite, 1);
            assertEquals(1, results.size());

            InterferenceEntry entry = results.getFirst();
            assertEquals(ResonanceZone.SHADOW, entry.zone());
            assertTrue(entry.energy() < 0.1f, "Energy should be near zero in destructive interference");
        } finally {
            TestUtils.deleteDirectoryRecursive(tempDir);
        }
    }

    @Test
    void testInterferenceMapSizeRespectsTopK() throws Exception {
        Path tempDir = Files.createTempDirectory("resonance-test");
        try (WavePatternStoreImpl store = new WavePatternStoreImpl(tempDir)) {

            for (int i = 0; i < 10; i++) {
                WavePattern p = WavePatternTestUtils.createConstantPattern(1.0, i * 0.1, 64);
                store.insert(p, Map.of("index", String.valueOf(i)));
            }

            WavePattern query = WavePatternTestUtils.createConstantPattern(1.0, 0.0, 64);
            List<InterferenceEntry> results = store.queryInterferenceMap(query, 5);
            assertEquals(5, results.size());
        } finally {
            TestUtils.deleteDirectoryRecursive(tempDir);
        }
    }
}