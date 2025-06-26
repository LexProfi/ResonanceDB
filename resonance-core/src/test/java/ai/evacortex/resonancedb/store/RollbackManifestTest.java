/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.WavePatternTestUtils;
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.storage.*;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RollbackManifestTest {

    private Path tempDir;
    private WavePatternStoreImpl store;

    @BeforeEach
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("resdb-test-rollback");
        store = new WavePatternStoreImpl(tempDir);
    }

    @AfterEach
    public void tearDown() {
        store.close();
        deleteRecursively(tempDir.toFile());
    }

    private void deleteRecursively(java.io.File file) {
        if (file.isDirectory()) {
            for (java.io.File child : file.listFiles()) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    @Test
    public void testInsertFailureDoesNotPolluteManifest() {
        WavePattern pattern = WavePatternTestUtils.createRandomPattern(512, 1234L);
        assertDoesNotThrow(() -> store.insert(pattern, Map.of()));

        assertThrows(DuplicatePatternException.class, () -> store.insert(pattern, Map.of()));

        assertTrue(store.containsExactPattern(pattern));
    }

    @Test
    public void testReplaceFailureRollback() {
        WavePattern original = WavePatternTestUtils.createRandomPattern(512, 5678L);
        WavePattern conflicting = WavePatternTestUtils.createRandomPattern(512, 9012L);

        assertDoesNotThrow(() -> store.insert(original, Map.of("tag", "original")));
        store.insert(conflicting, Map.of("tag", "conflict"));

        assertThrows(DuplicatePatternException.class, () ->
                store.replace(HashingUtil.computeContentHash(original), conflicting, Map.of("new", "value")));

        assertTrue(store.containsExactPattern(original));
        assertTrue(store.containsExactPattern(conflicting));
    }
}