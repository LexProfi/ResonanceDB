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
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
        dir   = Files.createTempDirectory("resonance-rollback");
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
        String id      = store.insert(ok, Map.of());
        WavePattern bad = new WavePattern(
                new double[]{7, 7, 7},
                new double[]{0.7});

        assertThrows(RuntimeException.class, () -> store.update(id, bad, Map.of()),
                "update must propagate underlying failure");

        List<ResonanceMatch> res = store.query(ok, 1);

        assertFalse(res.isEmpty(), "old pattern must survive failed update");
        assertEquals(id, res.get(0).id());
        assertEquals(1.0f, res.get(0).energy(), 1e-5, "energy with original ψ should stay 1.0");

        long countInManifest = Files.readAllLines(dir.resolve("index/manifest.idx"))
                .stream()
                .filter(l -> l.contains(id))
                .count();
        assertEquals(1, countInManifest, "failed update must not duplicate entries in manifest");
    }
}
