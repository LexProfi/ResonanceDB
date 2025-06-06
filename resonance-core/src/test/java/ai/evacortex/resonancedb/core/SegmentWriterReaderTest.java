/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.io.format.BinaryHeader;
import ai.evacortex.resonancedb.core.storage.HashingUtil;
import ai.evacortex.resonancedb.core.storage.io.SegmentReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;


class SegmentWriterReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void testWriteAndReadSinglePattern() throws Exception {
        Path segmentFile = tempDir.resolve("test.segment");

        WavePattern pattern = WavePatternTestUtils.createConstantPattern(0.5, 1.0, 512);
        String idStr  = "pattern-123";
        String hexId  = HashingUtil.md5Hex(idStr);

        long offset;
        try (SegmentWriter writer = new SegmentWriter(segmentFile)) {
            offset = writer.write(hexId, pattern);
            writer.flush();
        }

        try (SegmentReader reader = new SegmentReader(segmentFile)) {
            BinaryHeader hdr = reader.getHeader();
            assertEquals(1, hdr.version());
            assertTrue(hdr.timestamp() > 0);
            assertEquals(1, hdr.recordCount());
            assertTrue(hdr.lastOffset() > BinaryHeader.SIZE);

            SegmentReader.PatternWithId loaded = reader.readWithId(offset);
            assertEquals(hexId, loaded.id());
            assertArrayEquals(pattern.amplitude(), loaded.pattern().amplitude(), 1e-9);
            assertArrayEquals(pattern.phase(),     loaded.pattern().phase(),     1e-9);
        }
    }
}