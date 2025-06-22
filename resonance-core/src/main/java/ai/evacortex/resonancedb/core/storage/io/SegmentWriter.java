/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.io;

import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.SegmentOverflowException;
import ai.evacortex.resonancedb.core.storage.HashingUtil;
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

/**
 * SegmentWriter is responsible for appending encoded {@link WavePattern} entries
 * to a memory-mapped segment file (*.segment) using a deterministic binary layout.
 *
 * <p>
 * Each segment begins with a {@link BinaryHeader}, which tracks versioning,
 * logical timestamps, pattern count, last write offset, checksum, and a commit marker.
 * Entries are serialized sequentially and aligned for efficient read access.
 * </p>
 *
 * <h3>Entry Layout</h3>
 * Each entry is written using the following binary structure:
 * <pre>
 * [ID_HASH (16 bytes)]         // Unique pattern identifier (e.g., content-derived hash)
 * [LENGTH (4 bytes)]           // Number of elements per vector
 * [RESERVED (4 bytes)]         // Reserved field for future use
 * [Pattern body]               // Encoded amplitude and phase components
 * [padding]                    // Optional alignment for access efficiency
 * </pre>
 *
 * <h3>Checksum Handling</h3>
 * Each segment includes an integrity checksum covering all record data
 * written after the header. The length of the checksum (4 or 8 bytes) is configurable.
 * The checksum is recalculated after every write or flush.
 * </p>
 *
 * <h3>Concurrency</h3>
 * SegmentWriter is thread-safe. Internally, it uses a {@link ReentrantReadWriteLock}
 * to synchronize write access and buffer state.
 *
 * <h3>Durability</h3>
 * Every {@link #write(String, WavePattern)} operation updates the header atomically
 * with the latest offset, record count, and checksum. On {@link #flush()} or {@link #close()},
 * the data is persisted via explicit sync and buffer unmapping.
 *
 * <h3>Lifecycle</h3>
 * The caller must invoke {@link #close()} to ensure proper resource release.
 * Intermediate durability can be triggered via {@link #flush()} or {@link #sync()}.
 *
 * <h3>Errors</h3>
 * Throws {@link SegmentOverflowException} when the pattern exceeds segment capacity.
 * Throws {@link InvalidWavePatternException} for malformed input or invalid ID format.
 *
 * @see SegmentReader
 * @see BinaryHeader
 * @see WavePattern
 * @see WavePatternCodec
 */
public class SegmentWriter implements AutoCloseable {

    private static final int ID_SIZE = 16;
    private static final int RECORD_HEADER_SIZE = 1 + 16 + 4 + 4;
    private static final int ALIGNMENT = 8;
    private static final int INITIAL_CAPACITY = 4 * 1024 * 1024;

    private final Path path;
    private final int headerSize;
    private final int checksumLength;
    private final String segmentName;
    private final FileChannel channel;
    private MappedByteBuffer buffer;
    private final AtomicLong writeOffset;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private int recordCount = 0;

    public SegmentWriter(Path path) {
        this(path, 8);
    }
    public SegmentWriter(Path path, int checksumLength) {
        try {
            this.path = path;
            this.checksumLength = checksumLength;
            this.headerSize = BinaryHeader.sizeFor(checksumLength);
            this.segmentName = path.getFileName().toString();
            Files.createDirectories(path.getParent());

            RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
            this.channel = raf.getChannel();

            boolean isNew = channel.size() == 0;
            long mapSize = Math.max(INITIAL_CAPACITY, channel.size());
            this.buffer = Buffers.mmap(channel, FileChannel.MapMode.READ_WRITE, 0, mapSize);
            this.buffer.order(ByteOrder.LITTLE_ENDIAN);

            if (isNew) {
                BinaryHeader header = new BinaryHeader(1, System.currentTimeMillis(), 0,
                        headerSize, 0L, (byte) 0, checksumLength);
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
            buffer.put(idBytes);             // [1..16]
            buffer.putInt(pattern.amplitude().length); // [17..20]
            buffer.putInt(-1);

            ByteBuffer tmpBuf = ByteBuffer.allocate(patternSize).order(ByteOrder.LITTLE_ENDIAN);
            WavePatternCodec.writeTo(tmpBuf, pattern, false);
            tmpBuf.flip();
            buffer.put(tmpBuf);

            writeOffset.addAndGet(alignedSize);
            recordCount++;

            int checksumOffset = headerSize;
            int lengthToChecksum = (int) (writeOffset.get() - checksumOffset);

            ByteBuffer checksumBuf = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            checksumBuf.position(checksumOffset);
            checksumBuf.limit(checksumOffset + lengthToChecksum);
            long checksum = HashingUtil.computeChecksum(checksumBuf.slice(), checksumLength);

            BinaryHeader header = new BinaryHeader(1, System.currentTimeMillis(), recordCount,
                    writeOffset.get(), checksum, (byte) 1, checksumLength
            );

            ensureCapacity(headerSize);
            buffer.position(0);
            buffer.put(header.toBytes());
            buffer.force(0, headerSize);

            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void markDeleted(long offset) {
        lock.writeLock().lock();
        try {
            if (offset < buffer.limit()) {
                buffer.put((int) offset, (byte) 0x00);  // пометить как удалённую
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
            buffer.position((int) offset);           // первый байт записи
            buffer.put((byte) 0x01);               // установить как "валиден"
            buffer.force();
            channel.force(false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to unmark deleted at offset: " + offset, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void flush() {
        lock.writeLock().lock();
        try {
            int lengthToChecksum = (int) (writeOffset.get() - headerSize);

            ByteBuffer checksumBuf = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            checksumBuf.position(headerSize);
            checksumBuf.limit(headerSize + lengthToChecksum);

            long checksum = HashingUtil.computeChecksum(checksumBuf.slice(), checksumLength);

            BinaryHeader header = new BinaryHeader(
                    1,
                    System.currentTimeMillis(),
                    recordCount,
                    writeOffset.get(),
                    checksum,
                    (byte) 1,
                    checksumLength
            );

            if (buffer != null) {
                ensureCapacity(headerSize);
                buffer.position(0);
                buffer.put(header.toBytes());
                buffer.force(0, headerSize);
            }
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
                throw new SegmentOverflowException("Failed to remap segment buffer", e);
            }
        }
    }

    public String getSegmentName() {
        return segmentName;
    }

    public Path getPath() {
        return path;
    }

    public boolean willOverflow(WavePattern pattern) {
        int patternSize = WavePatternCodec.estimateSize(pattern, false);
        int blockSize = RECORD_HEADER_SIZE + patternSize;
        int aligned = align(blockSize);
        return writeOffset.get() + aligned > buffer.capacity();
    }

    public boolean isOverflow() {
        long remaining = buffer.capacity() - writeOffset.get();
        return remaining < 32 * 1024;
    }

    public double getFillRatio() {
        return (double) writeOffset.get() / buffer.capacity();
    }

    public long getWriteOffset() {
        return writeOffset.get();
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

    public void sync() {
        lock.writeLock().lock();
        try {
            buffer.force();
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to sync segment to disk", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long approxSize() { return writeOffset.get(); }
}