/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RollbackManifestTest {

    private static final String PROP_PATTERN_LEN = "resonance.pattern.len";

    private Path tempDir;
    private WavePatternStoreImpl store;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("resdb-test-rollback");
        store = new WavePatternStoreImpl(tempDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            if (store != null) store.close();
        } finally {
            if (tempDir != null) {
                TestUtils.deleteDirectoryRecursive(tempDir);
            }
        }
    }

    private static int len() {
        return Integer.getInteger(PROP_PATTERN_LEN, 1536);
    }

    @Test
    void testInsertFailureDoesNotPolluteManifest() {
        int patternLen = len();
        WavePattern pattern = WavePatternTestUtils.createRandomPattern(patternLen, 1234L);
        assertDoesNotThrow(() -> store.insert(pattern, Map.of()));
        assertThrows(DuplicatePatternException.class, () -> store.insert(pattern, Map.of()));
        assertTrue(store.containsExactPattern(pattern));
    }

    @Test
    void testReplaceFailureRollback() {
        int patternLen = len();
        WavePattern original = WavePatternTestUtils.createRandomPattern(patternLen, 5678L);
        WavePattern conflicting = WavePatternTestUtils.createRandomPattern(patternLen, 9012L);
        assertDoesNotThrow(() -> store.insert(original, Map.of("tag", "original")));
        store.insert(conflicting, Map.of("tag", "conflict"));
        String originalId = HashingUtil.computeContentHash(original);
        assertThrows(DuplicatePatternException.class, () ->
                store.replace(originalId, conflicting, Map.of("new", "value"))
        );
        assertTrue(store.containsExactPattern(original));
        assertTrue(store.containsExactPattern(conflicting));
    }
}