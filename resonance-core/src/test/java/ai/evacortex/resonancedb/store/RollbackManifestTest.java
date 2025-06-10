/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.metadata.PatternMetaStore;
import ai.evacortex.resonancedb.core.storage.*;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.WavePatternTestUtils;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
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
    @SuppressWarnings("resource, ResultOfMethodCallIgnored")
    void tearDown() throws Exception {
        store.close();
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void testInsertFailureRollback() {
        WavePattern bad = new WavePattern(
                new double[]{1, 2, 3, 4},
                new double[]{0.0}
        );

        assertThrows(IllegalArgumentException.class, () -> store.insert(bad, Map.of()),
                "insert must fail with invalid WavePattern");

        assertThrows(IllegalArgumentException.class, () -> store.query(bad, 1),
                "query must fail on invalid pattern with mismatched lengths");
    }

    @Test
    void testReplaceFailureRollback() throws Exception {
        WavePattern original = WavePatternTestUtils.createConstantPattern(0.2, 0.2, 16);
        String oldId = store.insert(original, Map.of("key", "original"));

        WavePattern updated = WavePatternTestUtils.createConstantPattern(0.3, 0.3, 16);
        String newId = HashingUtil.computeContentHash(updated);

        try {
            store.insert(updated, Map.of());
        } catch (Exception ignored) {}

        extractManifest(store).remove(newId);
        extractMeta(store).remove(newId);

        String segment = store.getShardSelector().selectShard(updated);
        String baseName = segment.split("-")[0];

        Map<String, PhaseSegmentGroup> groups = extractSegmentGroups(store);
        PhaseSegmentGroup group = groups.get(baseName);
        assertNotNull(group, "PhaseSegmentGroup must exist");

        Field writersField = PhaseSegmentGroup.class.getDeclaredField("writers");
        writersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<SegmentWriter> writers = (List<SegmentWriter>) writersField.get(group);

        SegmentWriter spy = Mockito.spy(writers.getFirst());
        Mockito.doThrow(new RuntimeException("simulated insert failure"))
                .when(spy).write(Mockito.eq(newId), Mockito.any());

        writers.set(0, spy);

        assertThrows(RuntimeException.class, () -> store.replace(oldId, updated, Map.of("key", "updated")));

        List<ResonanceMatch> matches = store.query(original, 1);
        assertFalse(matches.isEmpty());
        assertEquals(oldId, matches.getFirst().id());
        assertEquals(1.0f, matches.getFirst().energy(), 1e-5);

        ManifestIndex manifest = extractManifest(store);
        assertTrue(manifest.contains(oldId), "old ID must remain in manifest");
        assertFalse(manifest.contains(newId), "new ID must not appear after failed replace");

        PatternMetaStore meta = extractMeta(store);
        assertTrue(meta.contains(oldId), "old ID must remain in metadata");
        assertFalse(meta.contains(newId), "new ID must not appear in metadata");
    }

    private ManifestIndex extractManifest(WavePatternStoreImpl store) throws Exception {
        Field f = WavePatternStoreImpl.class.getDeclaredField("manifest");
        f.setAccessible(true);
        return (ManifestIndex) f.get(store);
    }

    private PatternMetaStore extractMeta(WavePatternStoreImpl store) throws Exception {
        Field f = WavePatternStoreImpl.class.getDeclaredField("metaStore");
        f.setAccessible(true);
        return (PatternMetaStore) f.get(store);
    }

    private Map<String, PhaseSegmentGroup> extractSegmentGroups(WavePatternStoreImpl store) throws Exception {
        Field f = WavePatternStoreImpl.class.getDeclaredField("segmentGroups");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, PhaseSegmentGroup> map = (Map<String, PhaseSegmentGroup>) f.get(store);
        return map;
    }
}