/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HashingUtilTest {

    @Test
    void testContentHash_isDeterministicAndContentSensitive() {
        WavePattern pattern1 = WavePatternTestUtils.fixedPattern();
        WavePattern pattern2 = WavePatternTestUtils.fixedPattern();

        String hash1 = HashingUtil.computeContentHash(pattern1);
        String hash2 = HashingUtil.computeContentHash(pattern2);

        assertEquals(hash1, hash2, "Content hash must be deterministic for identical patterns");

        assertEquals(32, hash1.length(), "MD5 hex string must be 32 chars long");
        assertEquals(16, HashingUtil.parseAndValidateMd5(hash1).length,
                "Parsed MD5 must contain exactly 16 bytes");

        WavePattern different = WavePatternTestUtils.createConstantPattern(0.123, 0.456, 8);
        String diffHash = HashingUtil.computeContentHash(different);
        assertNotEquals(hash1, diffHash, "Hashes must differ for different patterns");
    }
}