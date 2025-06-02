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
 *
 * Each segment starts with a {@link BinaryHeader}.
 * Each entry layout:
 *   [ID_HASH (16 bytes)] [LENGTH (4 bytes)] [META_OFFSET (4 bytes)] [WavePattern binary]
 *
 * Thread-safe: uses ReentrantReadWriteLock for concurrent read/write access.
 */
public class SegmentWriter implements AutoCloseable {

    private static final int FIXED_HEADER_SIZE = 16 + 4 + 4; // ID_HASH (16) + LEN (4) + META_OFFSET (4)
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
     * Writes a WavePattern entry with given ID and optional metadata.
     *
     * @param id       unique identifier for the pattern
     * @param pattern  the WavePattern to serialize
     * @param metadata optional metadata (currently unused)
     * @return byte offset where pattern was written
     */
    public long write(String id, WavePattern pattern, Map<String, String> metadata) {
        lock.writeLock().lock();
        try {
            byte[] idHash = HashingUtil.md5(id);

            int patternSize = WavePatternCodec.estimateSize(pattern, false);
            int blockSize = FIXED_HEADER_SIZE + patternSize;
            int alignedSize = align(blockSize);
            long offset = writeOffset.getAndAdd(alignedSize);

            ensureCapacity(offset + alignedSize);
            buffer.position((int) offset);

            buffer.put(idHash);                           // 16-byte MD5
            buffer.putInt(pattern.amplitude().length);    // pattern length
            buffer.putInt(-1);                            // reserved metaOffset

            ByteBuffer tmpBuf = ByteBuffer.allocate(patternSize).order(ByteOrder.LITTLE_ENDIAN);
            WavePatternCodec.writeTo(tmpBuf, pattern, false);
            tmpBuf.flip();
            buffer.put(tmpBuf);

            recordCount++;
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Marks an entry at given offset as deleted (tombstone).
     *
     * @param offset offset of the entry to mark deleted
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

    public Path getPath() {
        return path;
    }

    /**
     * Flushes header with current record count.
     */
    public void flush() {
        lock.writeLock().lock();
        try {
            buffer.position(0);
            buffer.putInt(1); // version
            buffer.putLong(System.currentTimeMillis());
            buffer.putInt(recordCount);
            buffer.force();
        } finally {
            lock.writeLock().unlock();
        }
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