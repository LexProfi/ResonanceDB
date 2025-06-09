/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.storage.HashingUtil;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.WavePatternTestUtils;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.io.File;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RollbackManifestTest {

    private Path dir;
    private WavePatternStoreImpl store;

    @BeforeEach
    void setUp() throws Exception {
        dir = Files.createTempDirectory("resonance-rollback");
        store = new WavePatternStoreImpl(dir);
    }

    @AfterEach
    void tearDown() throws Exception {
        store.close();
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void testInsertFailureRollback() throws Exception {
        int linesBefore = Files.readAllLines(dir.resolve("index/manifest.idx")).size();
        WavePattern bad = new WavePattern(
                new double[]{1, 2, 3, 4},
                new double[]{0.0});

        assertThrows(RuntimeException.class, () -> store.insert(bad, Map.of()),
                "insert must propagate underlying failure");

        int linesAfter = Files.readAllLines(dir.resolve("index/manifest.idx")).size();
        assertEquals(linesBefore, linesAfter, "manifest must stay intact after failed insert");

        assertThrows(IllegalArgumentException.class, () -> store.query(bad, 1),
                "query must fail on invalid pattern with mismatched lengths");
    }

    @Test
    void testReplaceFailureRollback() throws Exception {
        WavePattern original = WavePatternTestUtils.createConstantPattern(0.2, 0.2, 16);
        String oldId = store.insert(original, Map.of());

        WavePattern updated = WavePatternTestUtils.createConstantPattern(0.3, 0.3, 16);
        String newId = HashingUtil.computeContentHash(updated);
        String segment = store.getShardSelector().selectShard(updated);

        // Spy на SegmentWriter
        Field writersField = WavePatternStoreImpl.class.getDeclaredField("segmentWriters");
        writersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, SegmentWriter> writers = (Map<String, SegmentWriter>) writersField.get(store);

        SegmentWriter originalWriter = writers.get(segment);
        SegmentWriter spy = Mockito.spy(originalWriter);
        Mockito.doThrow(new RuntimeException("simulated insert failure"))
                .when(spy).write(Mockito.eq(newId), Mockito.any(WavePattern.class));
        writers.put(segment, spy);

        assertThrows(RuntimeException.class, () -> store.replace(oldId, updated, Map.of()));

        // Проверка, что старый паттерн остался
        List<ResonanceMatch> matches = store.query(original, 1);
        assertFalse(matches.isEmpty(), "original pattern must still be queryable");
        assertEquals(oldId, matches.get(0).id());
        assertEquals(1.0f, matches.get(0).energy(), 1e-5);

        // Проверка состояния manifest
        Field manifestField = WavePatternStoreImpl.class.getDeclaredField("manifest");
        manifestField.setAccessible(true);
        ManifestIndex manifest = (ManifestIndex) manifestField.get(store);

        assertTrue(manifest.contains(oldId), "old ID must still be present in manifest");
        assertFalse(manifest.contains(newId), "new ID must not be present after failed replace");
    }
}