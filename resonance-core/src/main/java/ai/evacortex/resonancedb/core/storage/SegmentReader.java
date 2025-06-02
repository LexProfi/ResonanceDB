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
import ai.evacortex.resonancedb.core.io.codec.WavePatternCodec;
import ai.evacortex.resonancedb.core.io.format.BinaryHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
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
public class SegmentReader implements AutoCloseable {

    private static final int ID_SIZE = 16;
    private static final int HEADER_SIZE = ID_SIZE + 4 + 4; // ID + length + metaOffset
    private static final int MAX_LENGTH = 65536;

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer mmap;
    private final BinaryHeader header;

    public SegmentReader(Path path) {
        this.path = path;
        try {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

            // Read header
            ByteBuffer headerBuf = mmap.duplicate();
            headerBuf.position(0);
            this.header = BinaryHeader.from(headerBuf);

        } catch (IOException e) {
            throw new RuntimeException("Failed to map segment: " + path, e);
        }
    }

    public WavePattern readAt(long offset) {
        return readWithId(offset).pattern();
    }

    public PatternWithId readWithId(long offset) {
        ByteBuffer buf = mmap.duplicate();
        buf.position((int) offset);

        byte[] idBytes = new byte[ID_SIZE];
        buf.get(idBytes);

        int len = buf.getInt();
        buf.getInt(); // skip metaOffset

        if (len <= 0 || len > MAX_LENGTH) {
            throw new IllegalStateException("Corrupted pattern length at offset " + offset);
        }

        int patternBytes = WavePatternCodec.estimateSize(len, false);
        if (buf.remaining() < patternBytes) {
            throw new IllegalStateException("Insufficient bytes to decode pattern at offset " + offset);
        }

        ByteBuffer slice = buf.slice();
        slice.limit(patternBytes);
        WavePattern pattern = WavePatternCodec.readFrom(slice, false);

        return new PatternWithId(bytesToHex(idBytes), pattern);
    }

    public List<PatternWithId> readAllWithId() {
        List<PatternWithId> result = new ArrayList<>();
        ByteBuffer buf = mmap.duplicate();
        buf.position(BinaryHeader.SIZE); // skip file header

        while (buf.remaining() >= HEADER_SIZE) {
            int entryStart = buf.position();
            byte firstByte = buf.get(entryStart);

            if (firstByte == 0x00) {
                buf.position(entryStart + 1);
                byte[] skipId = new byte[ID_SIZE - 1];
                buf.get(skipId);
                int len = buf.getInt();
                buf.getInt(); // metaOffset
                int skipBytes = HEADER_SIZE + WavePatternCodec.estimateSize(len, false);
                buf.position(entryStart + skipBytes);
                continue;
            }

            byte[] idBytes = new byte[ID_SIZE];
            buf.get(idBytes);
            int len = buf.getInt();
            buf.getInt(); // skip metaOffset

            if (len <= 0 || len > MAX_LENGTH || buf.remaining() < WavePatternCodec.estimateSize(len, false)) {
                break; // malformed or incomplete
            }

            ByteBuffer slice = buf.slice();
            int size = WavePatternCodec.estimateSize(len, false);
            slice.limit(size);
            WavePattern pattern = WavePatternCodec.readFrom(slice, false);
            buf.position(buf.position() + size);

            result.add(new PatternWithId(bytesToHex(idBytes), pattern));
        }

        return result;
    }

    public BinaryHeader getHeader() {
        return header;
    }

    public Path getPath() {
        return path;
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

    public record PatternWithId(String id, WavePattern pattern) {}
}