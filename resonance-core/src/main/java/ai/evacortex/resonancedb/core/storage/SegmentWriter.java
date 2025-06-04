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
import java.util.HexFormat;
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

    private static final int ID_SIZE = 16;
    private static final int FIXED_HEADER_SIZE = 16 + 4 + 4;
    private static final int ALIGNMENT = 8;
    private static final int INITIAL_CAPACITY = 4 * 1024 * 1024;

    private final Path path;
    private final String segmentName;
    private final FileChannel channel;
    private MappedByteBuffer buffer;
    private final AtomicLong writeOffset;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private int recordCount = 0;

    public SegmentWriter(Path path) {
        try {
            this.path = path;
            this.segmentName = path.getFileName().toString();
            Files.createDirectories(path.getParent());

            RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
            this.channel = raf.getChannel();

            boolean isNew = channel.size() == 0;
            long mapSize = Math.max(INITIAL_CAPACITY, channel.size());
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
            this.buffer.order(ByteOrder.LITTLE_ENDIAN);

            if (isNew) {
                BinaryHeader header = new BinaryHeader(1, System.currentTimeMillis(), 0, BinaryHeader.SIZE);
                buffer.put(header.toBytes());
                this.writeOffset = new AtomicLong(BinaryHeader.SIZE);
            } else {
                ByteBuffer hdr = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
                hdr.position(0);
                int version = hdr.getInt();
                long ts = hdr.getLong();
                this.recordCount = hdr.getInt();
                long lastOffset = hdr.getLong();
                this.writeOffset = new AtomicLong(lastOffset);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize segment writer", e);
        }
    }

    public long write(String hexId, WavePattern pattern) {
        lock.writeLock().lock();
        try {
            byte[] idBytes = HexFormat.of().parseHex(hexId);
            if (idBytes.length != 16) {
                throw new IllegalArgumentException("ID must be a 16-byte MD5 hex string (32 characters)");
            }
            int patternSize = WavePatternCodec.estimateSize(pattern, false);
            int blockSize = FIXED_HEADER_SIZE + patternSize;
            int alignedSize = align(blockSize);

            long offset = writeOffset.get();
            ensureCapacity(offset + alignedSize);

            buffer.position((int) offset);
            buffer.put(idBytes);
            buffer.putInt(pattern.amplitude().length);
            buffer.putInt(-1);

            ByteBuffer tmpBuf = ByteBuffer.allocate(patternSize).order(ByteOrder.LITTLE_ENDIAN);
            WavePatternCodec.writeTo(tmpBuf, pattern, false);
            tmpBuf.flip();
            buffer.put(tmpBuf);

            writeOffset.addAndGet(alignedSize);
            recordCount++;
            buffer.position(0);
            buffer.putInt(1);
            buffer.putLong(System.currentTimeMillis());
            buffer.putInt(recordCount);
            buffer.putLong(writeOffset.get());
            buffer.force(0, BinaryHeader.SIZE);
            System.out.printf("Writing at offset %d (cap=%d)%n", offset, buffer.capacity());
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void markDeleted(long offset) {
        lock.writeLock().lock();
        try {
            if (offset < buffer.limit()) {
                buffer.put((int) offset, (byte) 0x00);
            } else {
                throw new IllegalArgumentException("Offset exceeds segment capacity: " + offset);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unmarkDeleted(long offset) {
        lock.writeLock().lock();
        try {
            buffer.position((int) offset + ID_SIZE + 4);
            buffer.putInt(-1);
            buffer.force();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void flush() {
        lock.writeLock().lock();
        try {
            buffer.position(0);
            BinaryHeader header = new BinaryHeader(1, System.currentTimeMillis(), recordCount, writeOffset.get());
            buffer.put(header.toBytes());
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
                this.buffer.order(ByteOrder.LITTLE_ENDIAN);
            } catch (IOException e) {
                throw new RuntimeException("Failed to remap segment buffer", e);
            }
        }
    }

    public String getSegmentName() {
        return segmentName;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (buffer != null) {
                flush();
                buffer.force();
                Buffers.unmap(buffer);
                buffer = null;
            }
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close segment writer", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}