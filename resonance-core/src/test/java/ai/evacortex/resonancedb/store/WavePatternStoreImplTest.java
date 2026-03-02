/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.*;
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
import ai.evacortex.resonancedb.core.math.ResonanceZone;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class WavePatternStoreImplTest {

    private static final String PROP_PATTERN_LEN = "resonance.pattern.len";

    private WavePatternStoreImpl store;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup() {
        store = new WavePatternStoreImpl(tempDir);
    }

    @AfterEach
    void cleanup() {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    private static int len() {
        return Integer.getInteger(PROP_PATTERN_LEN, 1536);
    }

    private static WavePattern constant(double amp, double phase) {
        return WavePatternTestUtils.createConstantPattern(amp, phase, len());
    }

    private static WavePattern randomPattern(double ampMin, double ampMax, double phaseMin, double phaseMax, Random rnd) {
        int n = len();
        double[] a = new double[n];
        double[] p = new double[n];
        for (int i = 0; i < n; i++) {
            a[i] = ampMin + rnd.nextDouble() * (ampMax - ampMin);
            p[i] = phaseMin + rnd.nextDouble() * (phaseMax - phaseMin);
        }
        return new WavePattern(a, p);
    }

    private static void assertSamePattern(WavePattern expected, WavePattern actual) {
        assertNotNull(actual, "pattern must be materialized");
        assertArrayEquals(expected.amplitude(), actual.amplitude(), 1e-9);
        assertArrayEquals(expected.phase(), actual.phase(), 1e-9);
    }

    private static WavePatternStoreImpl newStore(Path dir) {
        return new WavePatternStoreImpl(dir);
    }


    @Test
    void testInsertAndQuery() {
        WavePattern psi = constant(0.7, 0.3);

        String id = store.insert(psi, Map.of());
        List<ResonanceMatch> results = store.query(psi, 1);
        assertEquals(1, results.size());

        ResonanceMatch match = results.getFirst();
        assertEquals(id, match.id());
        assertSamePattern(psi, match.pattern());
        assertEquals(1.0f, match.energy(), 1e-5);
    }

    @Test
    void testInsertWithIdAndDelete() {
        WavePattern psi = constant(0.1, 0.1);
        String id = HashingUtil.computeContentHash(psi);

        store.insert(psi, Map.of("source", "test"));
        List<ResonanceMatch> results = store.query(psi, 1);

        assertEquals(1, results.size());
        assertEquals(id, results.getFirst().id());

        store.delete(id);
        assertThrows(PatternNotFoundException.class, () -> store.delete(id));
    }

    @Test
    void testReplace() {
        WavePattern original = constant(0.3, 0.1);
        String oldId = store.insert(original, Map.of("rev", "1"));

        WavePattern updated = constant(0.9, 1.2);
        String newId = store.replace(oldId, updated, Map.of("rev", "2"));

        assertEquals(HashingUtil.computeContentHash(updated), newId);
        assertNotEquals(oldId, newId, "ID must change after content replacement");

        assertThrows(PatternNotFoundException.class, () -> store.delete(oldId),
                "Old ID must no longer exist");

        List<ResonanceMatch> matches = store.query(updated, 5);
        Optional<ResonanceMatch> found = matches.stream()
                .filter(m -> m.id().equals(newId))
                .findFirst();

        assertTrue(found.isPresent(), "Replaced pattern must be found by query");
        assertEquals(1.0f, store.compare(updated, found.get().pattern()), 1e-4);

        List<ResonanceMatch> residual = store.query(original, 1);
        if (!residual.isEmpty()) {
            assertNotEquals(oldId, residual.getFirst().id(), "Old ID must not appear");
            assertTrue(residual.getFirst().energy() < 0.95f, "Old pattern must not match strongly");
        }
    }

    @Test
    void testCompareDifferentPhases() {
        WavePattern a = constant(1.0, 0.0);
        WavePattern b = constant(1.0, Math.PI);
        float score = store.compare(a, b);
        assertTrue(score < 0.05f);
    }

    @Test
    void testCompareIdentical() {
        WavePattern a = constant(1.0, 0.0);
        WavePattern b = constant(1.0, 0.0);
        float score = store.compare(a, b);
        assertEquals(1.0f, score, 1e-5);
    }

    @Test
    void testInsertDuplicateThrows() {
        WavePattern psi = constant(0.5, 0.5);
        store.insert(psi, Map.of());
        assertThrows(DuplicatePatternException.class, () -> store.insert(psi, Map.of()));
    }

    @Test
    void testDeleteNonExistentThrows() {
        assertThrows(PatternNotFoundException.class, () ->
                store.delete("0123456789abcdef0123456789abcdef"));
    }

    @Test
    void testInvalidLengthPatternsRejected() {
        int n = len();
        int badLen1 = Math.max(1, n - 1);
        int badLen2 = n + 1;

        WavePattern p1 = WavePatternTestUtils.createConstantPattern(1.0, 0.1, badLen1);
        WavePattern p2 = WavePatternTestUtils.createConstantPattern(1.0, 0.1, badLen2);

        assertThrows(InvalidWavePatternException.class, () -> store.insert(p1, Map.of()));
        assertThrows(InvalidWavePatternException.class, () -> store.query(p2, 1));
    }

    @Test
    void testMultipleSegmentsCreated() throws IOException {
        int count = 40;

        int n = len();
        for (int i = 0; i < count; i++) {
            double phaseShift = i * 0.15;
            double ampBase = 1.0 + (i * 0.001);

            double[] amp = new double[n];
            double[] phase = new double[n];
            Arrays.fill(amp, ampBase);
            Arrays.fill(phase, phaseShift);

            WavePattern psi = new WavePattern(amp, phase);
            store.insert(psi, Map.of("index", String.valueOf(i)));
        }

        try (Stream<Path> stream = Files.list(tempDir.resolve("segments"))) {
            long segmentCount = stream.count();
            assertTrue(segmentCount >= 2, "Expected at least 2 segments to be created");
        }
    }

    @Test
    void testMetadataWritten() throws IOException {
        WavePattern psi = constant(0.6, 0.4);
        Map<String, String> meta = Map.of("type", "test", "source", "unit");

        String id = store.insert(psi, meta);
        String json = Files.readString(tempDir.resolve("metadata/pattern-meta.json"));

        assertTrue(json.contains("\"" + id + "\""), "Meta should contain pattern ID");
        assertTrue(json.contains("\"type\""), "Meta should contain 'type'");
        assertTrue(json.contains("\"unit\""), "Meta should contain 'unit'");
    }

    @Test
    void testInsertWithAutoId() {
        WavePattern psi = constant(0.11, 0.22);

        String id1 = store.insert(psi, Map.of());
        assertNotNull(id1);

        DuplicatePatternException ex = assertThrows(DuplicatePatternException.class,
                () -> store.insert(psi, Map.of()));
        assertTrue(ex.getMessage().contains(id1));
    }

    @Test
    void testPersistenceAfterReopen() throws Exception {
        Path dir = Files.createTempDirectory("resonance-persist");

        WavePatternStoreImpl first = null;
        WavePatternStoreImpl reopened = null;

        try {
            first = newStore(dir);

            WavePattern psi = constant(0.7, 0.3);
            String id = first.insert(psi, Map.of("case", "persist"));
            first.close();
            first = null;

            reopened = newStore(dir);

            List<ResonanceMatch> hits = reopened.query(psi, 1);
            assertFalse(hits.isEmpty(), "pattern must survive reopen");
            assertEquals(id, hits.getFirst().id(), "ID must match");
            assertEquals(1.0f, hits.getFirst().energy(), 1e-5,
                    "energy with identical ψ must be 1");

            String metaJson = Files.readString(dir.resolve("metadata/pattern-meta.json"));
            assertTrue(metaJson.contains("\"" + id + "\""));
            assertTrue(metaJson.contains("\"case\""));

        } finally {
            if (first != null) first.close();
            if (reopened != null) reopened.close();
            TestUtils.deleteDirectoryRecursive(dir);
        }
    }

    @Test
    void testDeleteRemovesFromQuery() throws Exception {
        Path dir = Files.createTempDirectory("resonance-delete");

        try (WavePatternStoreImpl localStore = newStore(dir)) {
            WavePattern psi = constant(0.42, 0.13);
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
            TestUtils.deleteDirectoryRecursive(dir);
        }
    }

    @Test
    void testTopKOrderingAndSelfPriority() {
        final int TOTAL = 10;
        final double AMP = 1.0;

        String[] ids = new String[TOTAL];
        for (int i = 0; i < TOTAL; i++) {
            double phaseShift = i * 0.2;
            WavePattern p = constant(AMP, phaseShift);
            ids[i] = store.insert(p, Map.of("idx", String.valueOf(i)));
        }

        int targetIdx = 3;
        WavePattern query = constant(AMP, targetIdx * 0.2);

        int TOP_K = 5;
        List<ResonanceMatch> hits = store.query(query, TOP_K);

        assertEquals(TOP_K, hits.size(), "must return exactly top-K results");
        assertEquals(ids[targetIdx], hits.getFirst().id(), "self-match should have highest priority");

        for (int i = 1; i < hits.size(); i++) {
            assertTrue(hits.get(i - 1).energy() >= hits.get(i).energy(),
                    "energies must be sorted in descending order");
        }

        assertEquals(1.0f, hits.getFirst().energy(), 1e-5, "self-match energy should be 1");
    }

    @Test
    void testQueryDetailedWithZonesAndPhaseShift() {
        WavePattern core = constant(1.0, 0.0);
        WavePattern fringe = constant(1.0, 0.5);
        WavePattern shadow = constant(1.0, Math.PI);

        String idCore = store.insert(core, Map.of("label", "core"));
        String idFringe = store.insert(fringe, Map.of("label", "fringe"));
        String idShadow = store.insert(shadow, Map.of("label", "shadow"));

        InterferenceMap map = store.queryInterference(core, 5);
        List<ResonanceMatchDetailed> matches = map.matches();

        assertEquals(core, map.query(), "query pattern must be preserved");

        boolean foundCore = false, foundFringe = false, foundShadow = false;

        for (ResonanceMatchDetailed match : matches) {
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
        WavePattern core = constant(1.0, 0.0 + Math.random() * 1e-4);
        WavePattern fringe = constant(1.0, 0.5 + Math.random() * 1e-4);
        WavePattern shadow = constant(1.0, Math.PI + Math.random() * 1e-4);

        String idCore = store.insert(core, Map.of());
        String idFringe = store.insert(fringe, Map.of());
        String idShadow = store.insert(shadow, Map.of());

        InterferenceMap map = store.queryInterference(core, 5);
        List<ResonanceMatchDetailed> matches = map.matches();

        double scoreCore = -1, scoreFringe = -1, scoreShadow = -1;

        for (ResonanceMatchDetailed m : matches) {
            if (m.id().equals(idCore)) scoreCore = m.zoneScore();
            if (m.id().equals(idFringe)) scoreFringe = m.zoneScore();
            if (m.id().equals(idShadow)) scoreShadow = m.zoneScore();
        }

        assertTrue(scoreCore >= 0.9, "core should have very high score");
        assertTrue(scoreFringe > 0.1 && scoreFringe < scoreCore, "fringe should be intermediate");
        assertTrue(scoreShadow < 0.05, "shadow should be near zero");
    }

    @Test
    void testCoreZoneWithIdenticalPattern() throws Exception {
        Path td = Files.createTempDirectory("resonance-test");
        try (WavePatternStoreImpl local = newStore(td)) {
            WavePattern pattern = constant(1.0, 0.0);
            String id = local.insert(pattern, Map.of("kind", "test"));

            List<InterferenceEntry> results = local.queryInterferenceMap(pattern, 1);
            assertEquals(1, results.size());

            InterferenceEntry entry = results.getFirst();
            assertEquals(id, entry.id());
            assertEquals(ResonanceZone.CORE, entry.zone());
            assertTrue(entry.phaseShift() < 1e-6, "Phase shift should be ≈ 0 for identical pattern");
            assertTrue(entry.energy() > 0.99f);
        } finally {
            TestUtils.deleteDirectoryRecursive(td);
        }
    }

    @Test
    void testFringeZoneWithShiftedPhase() throws Exception {
        Path td = Files.createTempDirectory("resonance-test");
        try (WavePatternStoreImpl local = newStore(td)) {
            WavePattern pattern = constant(1.0, 0.0);
            WavePattern slightlyShifted = constant(1.0, 0.45);

            local.insert(pattern, Map.of());
            List<InterferenceEntry> results = local.queryInterferenceMap(slightlyShifted, 1);

            assertEquals(1, results.size());
            InterferenceEntry entry = results.getFirst();
            assertEquals(ResonanceZone.FRINGE, entry.zone());
            assertTrue(Math.abs(entry.phaseShift()) > 0.05 && Math.abs(entry.phaseShift()) < Math.PI);
        } finally {
            TestUtils.deleteDirectoryRecursive(td);
        }
    }

    @Test
    void testShadowZoneWithOppositePhase() throws Exception {
        Path td = Files.createTempDirectory("resonance-test");
        try (WavePatternStoreImpl local = newStore(td)) {
            WavePattern pattern = constant(1.0, 0.0);
            WavePattern opposite = constant(1.0, Math.PI);

            local.insert(pattern, Map.of());
            List<InterferenceEntry> results = local.queryInterferenceMap(opposite, 1);

            assertEquals(1, results.size());
            InterferenceEntry entry = results.getFirst();
            assertEquals(ResonanceZone.SHADOW, entry.zone());
            assertTrue(entry.energy() < 0.1f, "Energy should be near zero in destructive interference");
        } finally {
            TestUtils.deleteDirectoryRecursive(td);
        }
    }

    @Test
    void testInterferenceMapSizeRespectsTopK() throws Exception {
        Path td = Files.createTempDirectory("resonance-test");
        try (WavePatternStoreImpl local = newStore(td)) {
            for (int i = 0; i < 10; i++) {
                WavePattern p = constant(1.0, i * 0.1);
                local.insert(p, Map.of("index", String.valueOf(i)));
            }

            WavePattern query = constant(1.0, 0.0);
            List<InterferenceEntry> results = local.queryInterferenceMap(query, 5);
            assertEquals(5, results.size());
        } finally {
            TestUtils.deleteDirectoryRecursive(td);
        }
    }

    @Test
    void testQueryDetailedWithPhaseAnalysis() throws Exception {
        Path td = Files.createTempDirectory("resonance-query-detailed");
        try (WavePatternStoreImpl local = newStore(td)) {
            WavePattern core = constant(1.0, 0.0);
            WavePattern fringe = constant(1.0, 0.45);
            WavePattern shadow = constant(1.0, Math.PI);

            String idCore = local.insert(core, Map.of("zone", "core"));
            String idFringe = local.insert(fringe, Map.of("zone", "fringe"));
            String idShadow = local.insert(shadow, Map.of("zone", "shadow"));

            List<ResonanceMatchDetailed> matches = local.queryDetailed(core, 3);

            boolean seenCore = false, seenFringe = false, seenShadow = false;

            for (ResonanceMatchDetailed match : matches) {
                if (match.id().equals(idCore)) {
                    seenCore = true;
                    assertEquals(ResonanceZone.CORE, match.zone());
                    assertTrue(match.energy() > 0.95f);
                    assertEquals(0.0, match.phaseDelta(), 1e-6);
                }
                if (match.id().equals(idFringe)) {
                    seenFringe = true;
                    assertEquals(ResonanceZone.FRINGE, match.zone());
                    assertTrue(match.energy() < 0.97f);
                    assertTrue(Math.abs(match.phaseDelta()) > 0.05 && Math.abs(match.phaseDelta()) < Math.PI);
                }
                if (match.id().equals(idShadow)) {
                    seenShadow = true;
                    assertEquals(ResonanceZone.SHADOW, match.zone());
                    assertTrue(match.energy() < 0.1f);
                    assertTrue(Math.abs(match.phaseDelta()) > 2.5);
                }
            }

            assertTrue(seenCore, "CORE match must be present");
            assertTrue(seenFringe, "FRINGE match must be present");
            assertTrue(seenShadow, "SHADOW match must be present");
        } finally {
            TestUtils.deleteDirectoryRecursive(td);
        }
    }

    @Test
    void testQueryCompositeBasic() throws Exception {
        Path td = Files.createTempDirectory("resonance-query-composite-basic");
        try (WavePatternStoreImpl local = newStore(td)) {
            WavePattern p1 = constant(1.0, 0.0);
            WavePattern p2 = constant(1.0, Math.PI);
            WavePattern p3 = constant(1.0, 0.5);

            String id1 = local.insert(p1, Map.of("label", "core"));
            String id2 = local.insert(p2, Map.of("label", "shadow"));
            String id3 = local.insert(p3, Map.of("label", "fringe"));

            List<WavePattern> components = List.of(p1, p3);
            List<Double> weights = List.of(0.6, 0.4);

            List<ResonanceMatch> results = local.queryComposite(components, weights, 3);

            assertEquals(3, results.size());
            Set<String> foundIds = results.stream().map(ResonanceMatch::id).collect(Collectors.toSet());

            assertTrue(foundIds.contains(id1), "should include core component");
            assertTrue(foundIds.contains(id3), "should include fringe component");

            results.stream()
                    .filter(m -> m.id().equals(id2))
                    .findFirst()
                    .ifPresent(m -> assertTrue(m.energy() < 0.2f,
                            "Shadow pattern should have low energy, not " + m.energy()));
        } finally {
            TestUtils.deleteDirectoryRecursive(td);
        }
    }

    @Test
    void testQueryCompositeDetailedZones() throws Exception {
        Path td = Files.createTempDirectory("resonance-query-composite-detailed-zones");
        try (WavePatternStoreImpl local = newStore(td)) {
            WavePattern core = constant(1.0, 0.0);
            WavePattern fringe = constant(1.0, 0.5);
            WavePattern shadow = constant(1.0, Math.PI);

            String idCore = local.insert(core, Map.of("zone", "core"));
            String idFringe = local.insert(fringe, Map.of("zone", "fringe"));
            String idShadow = local.insert(shadow, Map.of("zone", "shadow"));

            List<WavePattern> composite = List.of(core, fringe);
            List<Double> weights = List.of(0.5, 0.5);

            List<ResonanceMatchDetailed> matches = local.queryCompositeDetailed(composite, weights, 5);

            boolean seenCore = false, seenFringe = false, seenShadow = false;

            for (ResonanceMatchDetailed match : matches) {
                if (match.id().equals(idCore)) {
                    seenCore = true;
                    assertEquals(ResonanceZone.CORE, match.zone());
                }
                if (match.id().equals(idFringe)) {
                    seenFringe = true;
                    assertTrue(
                            match.zone() == ResonanceZone.FRINGE || match.zone() == ResonanceZone.CORE,
                            "Expected FRINGE or CORE zone for fringe match, got " + match.zone()
                    );
                }
                if (match.id().equals(idShadow)) {
                    seenShadow = true;
                    assertEquals(ResonanceZone.SHADOW, match.zone());
                    assertTrue(match.energy() < 0.15f, "Shadow should have low energy");
                }
            }

            assertTrue(seenCore, "CORE must appear in composite query");
            assertTrue(seenFringe, "FRINGE must appear in composite query");
            assertTrue(seenShadow, "SHADOW must still be detectable");
        } finally {
            TestUtils.deleteDirectoryRecursive(td);
        }
    }

    @Test
    void testQueryReturnsMatchesFromMultiplePhases() {
        WavePattern phaseA = constant(1.0, 0.3);
        WavePattern phaseB = constant(1.0, 1.7);
        WavePattern query = constant(1.0, 1.0);

        String idA = store.insert(phaseA, Map.of("label", "phaseA"));
        String idB = store.insert(phaseB, Map.of("label", "phaseB"));

        List<ResonanceMatch> results = store.query(query, 2);

        Set<String> returnedIds = results.stream()
                .map(ResonanceMatch::id)
                .collect(Collectors.toSet());

        assertTrue(returnedIds.contains(idA), "Result must contain phase A pattern");
        assertTrue(returnedIds.contains(idB), "Result must contain phase B pattern");
    }

    @Test
    void testSegmentDistributionByPhase() throws IOException {
        double[] phaseCenters = {0.1, 1.0, 2.0, 2.9};
        int n = len();

        for (int i = 0; i < phaseCenters.length; i++) {
            double[] amp = new double[n];
            double[] phase = new double[n];
            Arrays.fill(amp, 1.0 + i * 0.001);
            Arrays.fill(phase, phaseCenters[i]);

            WavePattern psi = new WavePattern(amp, phase);
            store.insert(psi, Map.of("index", String.valueOf(i)));
        }

        Path segmentsDir = tempDir.resolve("segments");
        try (Stream<Path> files = Files.list(segmentsDir)) {
            long count = files.filter(f -> f.getFileName().toString().endsWith(".segment")).count();
            assertTrue(count >= 2, "Expected at least 2 segments for phase diversity");
        }
    }

    @Test
    void testQueryDetailedEnergyAccuracy() {
        WavePattern a = constant(1.0, 0.3);
        WavePattern b = constant(1.0, 0.9);

        String idB = store.insert(b, Map.of("label", "b"));

        float expected = store.compare(a, b);

        List<ResonanceMatchDetailed> matches = store.queryDetailed(a, 5);
        Optional<ResonanceMatchDetailed> bMatch = matches.stream()
                .filter(m -> m.id().equals(idB))
                .findFirst();

        assertTrue(bMatch.isPresent(), "Expected pattern b in detailed query results");
        float actual = bMatch.get().energy();

        assertEquals(expected, actual, 1e-5, "Detailed match energy must match direct compare()");
    }
}