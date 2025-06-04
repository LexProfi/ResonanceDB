/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.ResonanceMatch;
import ai.evacortex.resonancedb.core.WavePattern;
import ai.evacortex.resonancedb.core.WavePatternTestUtils;
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

        ResonanceMatch match = results.get(0);
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
        assertEquals(id, results.get(0).id());

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
        assertEquals(id, matches.get(0).id(), "ID should remain unchanged after update");
        assertTrue(matches.get(0).energy() > 0.95f, "energy with itself must stay high");
        List<ResonanceMatch> oldMatch = store.query(original, 1);
        assertFalse(oldMatch.isEmpty(), "query should still return a match");
        assertEquals(id, oldMatch.get(0).id(), "same business object, so same ID");
        assertTrue(oldMatch.get(0).energy() < 0.95f,
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
            String matchedId = store.query(pattern, 1).get(0).id();
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
            assertEquals(id,  hits.get(0).id(), "ID must match");
            assertEquals(1.0f, hits.get(0).energy(), 1e-5,
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
        WavePatternStoreImpl localStore = new WavePatternStoreImpl(dir);

        try {
            WavePattern psi = WavePatternTestUtils.createConstantPattern(0.42, 0.13, 32);
            String id       = localStore.insert(psi, Map.of());
            assertEquals(id, localStore.query(psi, 1).get(0).id());
            localStore.delete(id);
            List<ResonanceMatch> hits = localStore.query(psi, 3);
            boolean idStillPresent = hits.stream().anyMatch(m -> m.id().equals(id));
            assertFalse(idStillPresent, "deleted pattern must not appear in subsequent query results");

            if (!hits.isEmpty()) {
                assertTrue(hits.get(0).energy() < 0.95f,
                        "energy with deleted ψ must drop below similarity threshold");
            }

            assertThrows(PatternNotFoundException.class, () -> localStore.delete(id));

        } finally {
            localStore.close();
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
        assertEquals(ids[targetIdx], hits.get(0).id(), "self-match should have highest priority");
        for (int i = 1; i < hits.size(); i++) {
            assertTrue(hits.get(i-1).energy() >= hits.get(i).energy(),
                    "energies must be sorted in descending order");
        }
        assertEquals(1.0f, hits.get(0).energy(), 1e-5, "self-match energy should be 1");
    }
}