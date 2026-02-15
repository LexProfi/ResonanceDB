/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.io;

import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.SegmentOverflowException;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.io.codec.WavePatternCodec;
import ai.evacortex.resonancedb.core.storage.io.format.BinaryHeader;

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

public class SegmentWriter implements AutoCloseable {

    private static final long MAX_SEG_BYTES = Long.parseLong(System.getProperty("resonance.segment.maxBytes", "" + (64L << 20)));
    private static final int RECORD_HEADER_SIZE = 1 + 16 + 4 + 4;
    private static final int ALIGNMENT = 8;

    private final Path path;
    private final String segmentName;
    private final FileChannel channel;
    private final AtomicLong writeOffset;
    private final ReentrantReadWriteLock lock;

    private MappedByteBuffer buffer;
    private int recordCount = 0;
    private final int headerSize;
    private final int checksumLength;

    public SegmentWriter(Path path) {
        this(path, 8);
    }
    public SegmentWriter(Path path, int checksumLength) {
        try {
            this.lock = new ReentrantReadWriteLock();
            this.path = path;
            this.checksumLength = checksumLength;
            this.headerSize = BinaryHeader.sizeFor(checksumLength);
            this.segmentName = path.getFileName().toString();
            Files.createDirectories(path.getParent());

            RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
            this.channel = raf.getChannel();

            boolean isNew = channel.size() == 0;
            long mapSize = Math.max(MAX_SEG_BYTES, channel.size());
            this.buffer = Buffers.mmap(channel, FileChannel.MapMode.READ_WRITE, 0, mapSize);
            this.buffer.order(ByteOrder.LITTLE_ENDIAN);

            if (isNew) {
                BinaryHeader header = new BinaryHeader(1, System.currentTimeMillis(), 0,
                        headerSize, 0L, (byte) 1, checksumLength);
                ensureCapacity(headerSize);
                buffer.position(0);
                buffer.put(header.toBytes());
                this.writeOffset = new AtomicLong(headerSize);
            } else {
                ByteBuffer hdr = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
                hdr.position(0);
                BinaryHeader header = BinaryHeader.from(hdr, checksumLength);
                this.recordCount = header.recordCount();
                this.writeOffset = new AtomicLong(header.lastOffset());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize segment writer", e);
        }
    }

    public long write(String hexId, WavePattern pattern) throws SegmentOverflowException {
        lock.writeLock().lock();
        try {
            byte[] idBytes = HexFormat.of().parseHex(hexId);
            if (idBytes.length != 16) {
                throw new InvalidWavePatternException("ID must be a 16-byte MD5 hex string (32 characters)");
            }

            int patternSize = WavePatternCodec.estimateSize(pattern, false);
            int blockSize = RECORD_HEADER_SIZE + patternSize;
            int alignedSize = align(blockSize);

            long offset = writeOffset.get();
            if (offset + alignedSize > buffer.capacity()) {
                throw new SegmentOverflowException("Not enough space in segment");
            }

            ensureCapacity(Math.max(offset + alignedSize, headerSize));

            buffer.position((int) offset);
            buffer.put((byte) 0x01);
            buffer.put(idBytes);
            buffer.putInt(pattern.amplitude().length);
            buffer.putInt(-1);

            WavePatternCodec.writeDirect(buffer, pattern);
            int pad = alignedSize - blockSize;
            for (int i = 0; i < pad; i++) {
                buffer.put((byte) 0);
            }

            writeOffset.addAndGet(alignedSize);
            recordCount++;

            int checksumOffset = headerSize;
            int lengthToChecksum = (int) (writeOffset.get() - checksumOffset);

            ByteBuffer checksumBuf = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            checksumBuf.position(checksumOffset);
            checksumBuf.limit(checksumOffset + lengthToChecksum);

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
                throw new IllegalStateException("Offset exceeds segment capacity: " + offset);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unmarkDeleted(long offset) {
        lock.writeLock().lock();
        try {
            buffer.position((int) offset);
            buffer.put((byte) 0x01);
            buffer.force();
            channel.force(false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to unmark deleted at offset: " + offset, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void ensureCapacity(long requiredCapacity) {
        if (requiredCapacity <= buffer.capacity()) return;
        if (requiredCapacity > buffer.capacity()) {
            try {
                long newSize = Math.max(buffer.capacity() * 2L, requiredCapacity);

                buffer.force();
                Buffers.unmap(buffer);
                this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
                this.buffer.order(ByteOrder.LITTLE_ENDIAN);
            } catch (IOException e) {
                throw new SegmentOverflowException("Failed to remap segment buffer", e);
            }
        }
    }

    public boolean willOverflow(WavePattern pattern) {
        int patternSize = WavePatternCodec.estimateSize(pattern, false);
        int blockSize = RECORD_HEADER_SIZE + patternSize;
        int aligned = align(blockSize);
        return writeOffset.get() + aligned > buffer.capacity();
    }

    public long flush() {
        lock.writeLock().lock();
        try {
            if (buffer == null) {
                throw new IllegalStateException("Segment buffer is null during flush");
            }
            int lengthToChecksum = (int) (writeOffset.get() - headerSize);
            if (lengthToChecksum < 0) {
                throw new IllegalStateException("Invalid checksum length: " + lengthToChecksum);
            }

            ByteBuffer checksumBuf = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            checksumBuf.position(headerSize);
            checksumBuf.limit(headerSize + lengthToChecksum);
            long checksum = HashingUtil.computeChecksum(checksumBuf.slice(), checksumLength);

            long finalOffset = writeOffset.get();
            BinaryHeader header = new BinaryHeader(
                    1, System.currentTimeMillis(), recordCount, finalOffset,
                    checksum, (byte) 1, checksumLength);

            buffer.position(0);
            buffer.put(header.toBytes());

            buffer.force();
            return writeOffset.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void sync() {
        lock.writeLock().lock();
        try {
            if (buffer != null) {
                buffer.force();
            }
            if (channel.isOpen()) {
                channel.force(true);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to sync segment to disk", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int align(int size) {
        return ((size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public Path getPath() {
        return path;
    }

    public double getFillRatio() {
        return (double) writeOffset.get() / buffer.capacity();
    }

    public long getWriteOffset() {
        return writeOffset.get();
    }

    public long approxSize() {
        return writeOffset.get();
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (buffer != null) {
                flush();
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