/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.WavePattern;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.io.codec.WavePatternCodec;
import ai.evacortex.resonancedb.core.io.format.BinaryHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;


/**
 * SegmentReader allows reading serialized {@link WavePattern}s
 * from a memory-mapped .segment file produced by {@link SegmentWriter}.
 *
 * Each segment starts with a {@link BinaryHeader}.
 * Each entry layout:
 *   [ID_HASH (16 bytes)] [LENGTH (4 bytes)] [META_OFFSET (4 bytes)] [WavePattern binary]
 *
 * Deleted entries (tombstones) are marked with 0x00 at the first byte.
 * This class is read-only and thread-safe via duplicated buffer strategy.
 */
public final class SegmentReader implements AutoCloseable {

    private static final int ID_SIZE = 16;
    private static final int HEADER_SIZE = ID_SIZE + 4 + 4;
    private static final int MAX_LENGTH = 65_536;
    private static final int ALIGNMENT = 8;

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer mmap;
    private final BinaryHeader header;

    public SegmentReader(Path path) {
        this.path = path;
        try {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

            ByteBuffer hdrBuf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            hdrBuf.position(0);
            this.header = BinaryHeader.from(hdrBuf);

        } catch (IOException e) {
            throw new RuntimeException("Failed to map segment: " + path, e);
        }
    }

    public WavePattern readAt(long offset) {
        return readWithId(offset).pattern();
    }

    public PatternWithId readWithId(long offset) {
        if (offset < 0 || offset + HEADER_SIZE >= header.lastOffset()) {
            throw new InvalidWavePatternException("Offset out of segment bounds: " + offset);
        }

        ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        buf.position((int) offset);
        if (buf.get(buf.position()) == 0x00) {
            throw new PatternNotFoundException("Deleted (tombstoned) pattern at offset " + offset);
        }
        byte[] idBytes = new byte[ID_SIZE];
        buf.get(idBytes);
        int len = buf.getInt();
        buf.getInt();
        if (len <= 0 || len > MAX_LENGTH) {
            throw new InvalidWavePatternException("Invalid pattern length at offset " + offset);
        }
        int patternBytes = WavePatternCodec.estimateSize(len, false);
        if (buf.remaining() < patternBytes) {
            throw new InvalidWavePatternException("Insufficient bytes to decode pattern at offset " + offset);
        }

        ByteBuffer patternBuf = buf.slice();
        patternBuf.limit(patternBytes);
        WavePattern pattern = WavePatternCodec.readFrom(patternBuf,  false);

        return new PatternWithId(bytesToHex(idBytes), pattern, offset);
    }

    public List<PatternWithId> readAllWithId() {
        List<PatternWithId> result = new ArrayList<>();
        ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        buf.position(BinaryHeader.SIZE);

        while (buf.position() < header.lastOffset()) {
            int entryStart = buf.position();

            if (buf.remaining() < HEADER_SIZE) {
                throw new InvalidWavePatternException("Corrupted segment: insufficient space for header at offset " + entryStart);
            }

            byte firstByte = buf.get(entryStart);
            if (firstByte == 0x00) {
                // Tombstone entry — skip
                buf.position(entryStart + ID_SIZE);
                if (buf.remaining() < 8) {
                    throw new InvalidWavePatternException("Corrupted tombstone: can't read length at offset " + entryStart);
                }

                int len = buf.getInt();
                buf.getInt(); // skip tombstone marker
                if (len <= 0 || len > MAX_LENGTH) {
                    throw new InvalidWavePatternException("Invalid pattern length in tombstone at offset " + entryStart + ": " + len);
                }

                int skip = align(HEADER_SIZE + WavePatternCodec.estimateSize(len, false));
                buf.position(entryStart + skip);
                continue;
            }

            // Live entry
            buf.position(entryStart);
            byte[] idBytes = new byte[ID_SIZE];
            buf.get(idBytes);
            int len = buf.getInt();
            buf.getInt(); // tombstone marker, must be -1

            if (len <= 0 || len > MAX_LENGTH) {
                throw new InvalidWavePatternException("Invalid pattern length at offset " + entryStart + ": " + len);
            }

            int patternSize = WavePatternCodec.estimateSize(len, false);
            if (buf.remaining() < patternSize) {
                throw new InvalidWavePatternException("Insufficient bytes for pattern at offset " + entryStart + ": need " + patternSize + ", found " + buf.remaining());
            }

            ByteBuffer patternSlice = buf.slice();
            patternSlice.limit(patternSize);
            WavePattern pattern = WavePatternCodec.readFrom(patternSlice, false);

            int skip = align(HEADER_SIZE + patternSize);
            buf.position(entryStart + skip);

            result.add(new PatternWithId(bytesToHex(idBytes), pattern, entryStart));
        }

        System.out.println("OK readAllWithId returned " + result.size() + " entries");
        return result;
    }

    public BinaryHeader getHeader() {
        return header;
    }

    public Path getPath() {
        return path;
    }

    private static int align(int size) {
        return ((size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(ID_SIZE * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public record PatternWithId(String id, WavePattern pattern, long offset) {}
}