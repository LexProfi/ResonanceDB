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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SegmentWriter is responsible for low-level binary serialization and storage
 * of {@link WavePattern} entries into a memory-mapped segment file (.segment).

 * Each segment starts with a {@link BinaryHeader}.
 * Each entry layout:
 *   [ID_HASH (16 bytes)] [LENGTH (4 bytes)] [META_OFFSET (4 bytes)]
 *   [AMPLITUDES (8 * len bytes)] [PHASES (8 * len bytes)]

 * Thread-safe: uses ReentrantReadWriteLock for concurrent read/write access.
 */
public class SegmentWriter implements AutoCloseable {

    private static final int HEADER_SIZE = 16 + 4 + 4; // ID (16) + length + metaOffset
    private static final int ALIGNMENT = 8;
    private static final int INITIAL_CAPACITY = 4 * 1024 * 1024; // 4 MB

    private final Path path;
    private final String segmentName;
    private final FileChannel channel;
    private MappedByteBuffer buffer;
    private final AtomicLong writeOffset;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final boolean isNew;
    private int recordCount = 0;

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
            Files.createDirectories(path.getParent());

            RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
            this.channel = raf.getChannel();

            this.isNew = channel.size() == 0;
            long size = Math.max(INITIAL_CAPACITY, channel.size());
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);

            if (isNew) {
                BinaryHeader header = new BinaryHeader(1, System.currentTimeMillis(), 0);
                buffer.put(header.toBytes());
                this.writeOffset = new AtomicLong(BinaryHeader.SIZE);
            } else {
                ByteBuffer hdr = buffer.duplicate();
                hdr.order(ByteOrder.LITTLE_ENDIAN);
                hdr.position(0);
                hdr.getInt();      // version
                hdr.getLong();     // timestamp
                this.recordCount = hdr.getInt();
                this.writeOffset = new AtomicLong(channel.size());
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize segment writer", e);
        }
    }

    /**
     * Writes a WavePattern into the segment file at the current write offset.
     * ID is hashed to 16 bytes (MD5). Metadata is currently stored externally.
     *
     * @param id       Unique identifier for the pattern (hashed)
     * @param pattern  WavePattern to store
     * @param metadata Optional metadata (reserved for offset tracking)
     * @return Offset within the segment file where the pattern was written
     */
    public long write(String id, WavePattern pattern, Map<String, String> metadata) {
        lock.writeLock().lock();
        try {
            byte[] idHash = HashingUtil.md5(id);
            double[] amp = pattern.amplitude();
            double[] phase = pattern.phase();

            int len = amp.length;
            int blockSize = HEADER_SIZE + 8 * len * 2;
            int alignedSize = align(blockSize);
            long offset = writeOffset.getAndAdd(alignedSize);

            ensureCapacity(offset + alignedSize);
            buffer.position((int) offset);

            buffer.put(idHash);     // 16-byte MD5
            buffer.putInt(len);     // number of elements
            buffer.putInt(-1);      // placeholder for metadata offset

            for (double v : amp) buffer.putDouble(v);
            for (double v : phase) buffer.putDouble(v);

            recordCount++;
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

    public String getSegmentName() {
        return segmentName;
    }

    private int align(int size) {
        return ((size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
    }

    private void ensureCapacity(long requiredCapacity) {
        if (requiredCapacity > buffer.capacity()) {
            try {
                long newSize = Math.max(buffer.capacity() * 2L, requiredCapacity);
                buffer.force();
                this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
            } catch (IOException e) {
                throw new RuntimeException("Failed to remap segment buffer", e);
            }
        }
    }

    public Path getPath() {
        return path;
    }

    public void flush() {
        lock.writeLock().lock();
        try {
            // update BinaryHeader with new recordCount
            buffer.position(0);
            buffer.putInt(1); // version
            buffer.putLong(System.currentTimeMillis()); // updated timestamp
            buffer.putInt(recordCount); // updated count

            buffer.force();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            flush();
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close segment writer", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}