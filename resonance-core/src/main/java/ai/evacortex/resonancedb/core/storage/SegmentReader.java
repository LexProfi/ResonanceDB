/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.WavePattern;

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
 * Each entry layout:
 *   [ID_HASH (16 bytes)] [LENGTH (4 bytes)] [META_OFFSET (4 bytes)]
 *   [AMPLITUDES] [PHASES]
 *
 * Deleted entries (tombstones) are marked with 0x00 at the first byte.
 */
public class SegmentReader implements AutoCloseable {

    private static final int ID_SIZE = 16;
    private static final int HEADER_SIZE = ID_SIZE + 4 + 4; // ID + length + metaOffset
    private static final int MAX_LENGTH = 65536;

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer mmap;

    /**
     * Constructs a SegmentReader over a memory-mapped segment file.
     */
    public SegmentReader(Path path) {
        this.path = path;
        try {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        } catch (IOException e) {
            throw new RuntimeException("Failed to map segment: " + path, e);
        }
    }

    /**
     * Reads a WavePattern from a given offset.
     */
    public WavePattern readAt(long offset) {
        return readWithId(offset).pattern();
    }

    /**
     * Reads WavePattern and its internal ID (16-byte hash as hex string).
     */
    public PatternWithId readWithId(long offset) {
        ByteBuffer buf = mmap.duplicate();
        buf.position((int) offset);

        byte[] idBytes = new byte[ID_SIZE];
        buf.get(idBytes);
        String idHex = bytesToHex(idBytes);

        int length = buf.getInt();
        buf.getInt(); // skip metaOffset

        if (length <= 0 || length > MAX_LENGTH) {
            throw new IllegalStateException("Corrupted pattern length at offset " + offset);
        }

        double[] amp = new double[length];
        double[] phase = new double[length];
        for (int i = 0; i < length; i++) amp[i] = buf.getDouble();
        for (int i = 0; i < length; i++) phase[i] = buf.getDouble();

        return new PatternWithId(idHex, new WavePattern(amp, phase));
    }

    /**
     * Reads all valid (non-deleted) WavePatterns with their IDs from the segment.
     */
    public List<PatternWithId> readAllWithId() {
        List<PatternWithId> result = new ArrayList<>();
        ByteBuffer buf = mmap.duplicate();
        buf.position(0);

        while (buf.remaining() >= HEADER_SIZE) {
            int start = buf.position();
            byte tombstone = buf.get(start);
            if (tombstone == 0x00) {
                buf.position(start + ID_SIZE); // skip ID
                int len = buf.getInt();
                int skip = HEADER_SIZE + len * 2 * Double.BYTES;
                buf.position(start + skip);
                continue;
            }

            byte[] idBytes = new byte[ID_SIZE];
            buf.get(idBytes);
            String idHex = bytesToHex(idBytes);

            int len = buf.getInt();
            buf.getInt(); // skip metaOffset

            if (len <= 0 || len > MAX_LENGTH || buf.remaining() < len * 2 * Double.BYTES) break;

            double[] amp = new double[len];
            double[] phase = new double[len];
            for (int i = 0; i < len; i++) amp[i] = buf.getDouble();
            for (int i = 0; i < len; i++) phase[i] = buf.getDouble();

            result.add(new PatternWithId(idHex, new WavePattern(amp, phase)));
        }

        return result;
    }

    /**
     * Returns the path of this segment file.
     */
    public Path getPath() {
        return path;
    }

    /**
     * Closes the underlying FileChannel. Note: MappedByteBuffer is not explicitly unmapped
     * due to JVM limitations. It will be released when GC collects it.
     */
    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(ID_SIZE * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /**
     * Result of reading a pattern with ID.
     * @param id Hex-encoded 16-byte content hash
     * @param pattern WavePattern data
     */
    public record PatternWithId(String id, WavePattern pattern) {}
}