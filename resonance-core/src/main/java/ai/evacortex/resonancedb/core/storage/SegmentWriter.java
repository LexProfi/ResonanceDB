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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SegmentWriter is responsible for low-level binary serialization and storage
 * of {@link WavePattern} entries into a memory-mapped segment file (.segment).
 *
 * Each entry layout:
 *   [ID_HASH (16 bytes)] [LENGTH (4 bytes)] [META_OFFSET (4 bytes)]
 *   [AMPLITUDES (8 * len bytes)] [PHASES (8 * len bytes)]
 *
 * Thread-safe: uses ReentrantReadWriteLock for concurrent read/write access.
 */
public class SegmentWriter {

    private static final int HEADER_SIZE = 16 + 4 + 4; // ID (16) + length + metaOffset
    private static final int ALIGNMENT = 8;

    private final Path path;
    private final String segmentName;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final AtomicLong writeOffset;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Creates a new SegmentWriter bound to a specific .segment file.
     * Initializes a memory-mapped write buffer for fast access.
     *
     * @param path Path to the .segment file
     */
    public SegmentWriter(Path path) {
        try {
            this.path = path;
            this.segmentName = path.getFileName().toString();
            RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
            this.channel = raf.getChannel();
            long size = Math.max(1024 * 1024, channel.size());
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            this.writeOffset = new AtomicLong(channel.size());
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize segment writer", e);
        }
    }

    /**
     * Writes a WavePattern into the segment file at the current write offset.
     * ID is hashed to 16 bytes (MD5). Metadata is currently not stored.
     *
     * @param id       Unique identifier for the pattern (hashed)
     * @param pattern  WavePattern to store
     * @param metadata Optional metadata (not yet used)
     * @return Offset within the segment file where the pattern was written
     */
    public long write(String id, WavePattern pattern, Map<String, String> metadata) {
        lock.writeLock().lock();
        try {
            byte[] idHash = HashingUtil.md5(id); // 16 bytes
            double[] amp = pattern.amplitude();
            double[] phase = pattern.phase();

            int len = amp.length;
            int blockSize = HEADER_SIZE + 8 * len * 2;
            long offset = writeOffset.getAndAdd(align(blockSize));

            buffer.position((int) offset);
            buffer.put(idHash);      // 16-byte MD5
            buffer.putInt(len);      // number of elements
            buffer.putInt(-1);       // placeholder for metadata offset

            for (double v : amp) buffer.putDouble(v);
            for (double v : phase) buffer.putDouble(v);

            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Marks an existing pattern as deleted by setting its first byte to 0x00.
     *
     * @param offset Offset in the segment where the entry starts
     */
    public void markDeleted(long offset) {
        lock.writeLock().lock();
        try {
            buffer.put((int) offset, (byte) 0x00);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the name of the segment (filename).
     *
     * @return segment file name
     */
    public String getSegmentName() {
        return segmentName;
    }

    /**
     * Ensures alignment to 8-byte boundary for proper memory layout.
     *
     * @param size Size to align
     * @return Aligned size (multiple of ALIGNMENT)
     */
    private int align(int size) {
        return ((size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
    }

    /**
     * Returns the path of the segment file on disk.
     *
     * @return path to segment
     */
    public Path getPath() {
        return path;
    }
}