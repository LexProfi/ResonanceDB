/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.ResonanceMatch;
import ai.evacortex.resonancedb.core.WavePattern;
import ai.evacortex.resonancedb.core.WavePatternTestUtils;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;
import ai.evacortex.resonancedb.core.storage.SegmentWriter;
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
    void testUpdateFailureRollback() throws Exception {
        WavePattern ok = WavePatternTestUtils.createConstantPattern(0.2, 0.2, 16);
        String id = store.insert(ok, Map.of());
        Field manifestField = WavePatternStoreImpl.class.getDeclaredField("manifest");

        manifestField.setAccessible(true);

        ManifestIndex manifest = (ManifestIndex) manifestField.get(store);
        manifest.flush();

        WavePattern updated = WavePatternTestUtils.createConstantPattern(0.3, 0.3, 16);
        Field writersField = WavePatternStoreImpl.class.getDeclaredField("segmentWriters");
        writersField.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, SegmentWriter> writers = (Map<String, SegmentWriter>) writersField.get(store);
        String targetSegment = store.getShardSelector().selectShard(updated);

        store.query(updated, 1);

        SegmentWriter original = writers.get(targetSegment);
        SegmentWriter spy = Mockito.spy(original);
        Mockito.doThrow(new RuntimeException("simulated write failure"))
                .when(spy).write(Mockito.eq(id), Mockito.any(WavePattern.class));

        writers.put(targetSegment, spy);
        assertThrows(RuntimeException.class, () -> store.update(id, updated, Map.of()));

        List<ResonanceMatch> res = store.query(ok, 1);

        assertFalse(res.isEmpty(), "old pattern must survive failed update");
        assertEquals(id, res.get(0).id());
        assertEquals(1.0f, res.get(0).energy(), 1e-5);
        assertTrue(manifest.contains(id), "ID must still be present in manifest after failed update");
    }
}