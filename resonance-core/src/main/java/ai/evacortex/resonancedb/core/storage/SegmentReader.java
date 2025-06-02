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

 * Each segment starts with a {@link BinaryHeader}.
 * Each entry layout:
 *   [ID_HASH (16 bytes)] [LENGTH (4 bytes)] [META_OFFSET (4 bytes)]
 *   [AMPLITUDES] [PHASES]

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

    /**
     * Constructs a SegmentReader over a memory-mapped segment file.
     *
     * @param path path to the .segment file
     */
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

    /**
     * Reads a WavePattern from a given offset.
     *
     * @param offset byte offset inside the segment file (must be ≥ BinaryHeader.SIZE)
     * @return reconstructed WavePattern
     */
    public WavePattern readAt(long offset) {
        return readWithId(offset).pattern();
    }

    /**
     * Reads WavePattern and its 16-byte hex ID from a given offset.
     *
     * @param offset file offset to start reading
     * @return PatternWithId record containing ID and WavePattern
     */
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

        double[] amp = new double[len];
        double[] phase = new double[len];
        for (int i = 0; i < len; i++) amp[i] = buf.getDouble();
        for (int i = 0; i < len; i++) phase[i] = buf.getDouble();

        return new PatternWithId(bytesToHex(idBytes), new WavePattern(amp, phase));
    }

    /**
     * Reads all valid (non-deleted) WavePatterns with their IDs from the segment.
     *
     * @return list of valid patterns with their IDs
     */
    public List<PatternWithId> readAllWithId() {
        List<PatternWithId> result = new ArrayList<>();
        ByteBuffer buf = mmap.duplicate();
        buf.position(BinaryHeader.SIZE); // skip file header

        while (buf.remaining() >= HEADER_SIZE) {
            int entryStart = buf.position();
            byte firstByte = buf.get(entryStart);

            // Check tombstone
            if (firstByte == 0x00) {
                buf.position(entryStart + 1); // skip 0x00 marker
                byte[] skipId = new byte[ID_SIZE - 1];
                buf.get(skipId);
                int len = buf.getInt();
                int skip = HEADER_SIZE + len * 2 * Double.BYTES;
                buf.position(entryStart + skip);
                continue;
            }

            byte[] idBytes = new byte[ID_SIZE];
            buf.get(idBytes);

            int len = buf.getInt();
            buf.getInt(); // skip metaOffset

            if (len <= 0 || len > MAX_LENGTH || buf.remaining() < len * 2 * Double.BYTES) break;

            double[] amp = new double[len];
            double[] phase = new double[len];
            for (int i = 0; i < len; i++) amp[i] = buf.getDouble();
            for (int i = 0; i < len; i++) phase[i] = buf.getDouble();

            result.add(new PatternWithId(bytesToHex(idBytes), new WavePattern(amp, phase)));
        }

        return result;
    }

    /**
     * Returns the parsed segment file header.
     */
    public BinaryHeader getHeader() {
        return header;
    }

    /**
     * Returns the path of this segment file.
     */
    public Path getPath() {
        return path;
    }

    /**
     * Closes the underlying file channel.
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
     *
     * @param id      16-byte hex content hash
     * @param pattern reconstructed WavePattern
     */
    public record PatternWithId(String id, WavePattern pattern) {}
}