/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.io;

import ai.evacortex.resonancedb.core.exceptions.IncompleteWriteException;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.io.codec.WavePatternCodec;
import ai.evacortex.resonancedb.core.storage.io.format.BinaryHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class SegmentReader implements AutoCloseable {

    private static final int ID_SIZE = 16;
    private static final int HEADER_SIZE = 1 + 16 + 4 + 4;
    private static final int MAX_LENGTH = 65_536;
    private static final int ALIGNMENT = 8;

    private final Path path;
    private final int headerSize;
    private final FileChannel channel;
    private final MappedByteBuffer mmap;
    private final BinaryHeader header;
    public record PatternWithId(String id, WavePattern pattern, long offset) {}

    public SegmentReader(Path path) {
        this(path, 8);
    }

    public SegmentReader(Path path, int checksumLength) {
        this.path = path;
        this.headerSize = BinaryHeader.sizeFor(checksumLength);

        try {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);

            verifyManifestVersion(path); // ← TODO: compare-and-swap manifest version check + mmap refresh if needed

            this.mmap = Buffers.mmap(channel, FileChannel.MapMode.READ_ONLY, 0, channel.size());

            ByteBuffer hdrBuf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            hdrBuf.position(0);
            this.header = BinaryHeader.from(hdrBuf, checksumLength);

            if (header.lastOffset() < headerSize) {
                throw new InvalidWavePatternException("Header lastOffset (" + header.lastOffset()
                        + ") is less than header size (" + headerSize + ")");
            }

            if (header.commitFlag() != 1) {
                throw new IncompleteWriteException("Segment " + path.getFileName()
                        + " not marked as committed (flag=" + header.commitFlag() + ")");
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to map segment: " + path, e);
        }
    }

    public PatternWithId readWithId(long offset) {
        if (offset < 0 || offset + HEADER_SIZE >= header.lastOffset()) {
            throw new InvalidWavePatternException("Offset out of segment bounds: " + offset);
        }

        ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        buf.position((int) offset);
        byte flag = buf.get();
        if (flag == 0x00) {
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

    public static PatternWithId readWithIdFromBuffer(ByteBuffer buf, long offset) {
        buf.position((int) offset);

        byte flag = buf.get();
        if (flag == 0x00) {
            throw new PatternNotFoundException("Tombstone at offset " + offset);
        }

        byte[] idBytes = new byte[ID_SIZE];
        buf.get(idBytes);

        int len = buf.getInt();
        buf.getInt();

        if (len <= 0 || len > MAX_LENGTH) {
            throw new InvalidWavePatternException("Bad pattern length at offset " + offset);
        }

        int patternSize = WavePatternCodec.estimateSize(len, false);
        if (buf.remaining() < patternSize) {
            throw new InvalidWavePatternException("Truncated pattern at offset " + offset);
        }

        ByteBuffer patternBuf = buf.slice();
        patternBuf.limit(patternSize);
        WavePattern pattern = WavePatternCodec.readFrom(patternBuf, false);

        return new PatternWithId(bytesToHex(idBytes), pattern, offset);
    }

    public List<PatternWithId> readAllWithId() {
        Map<String, PatternWithId> latest = new LinkedHashMap<>();
        ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        buf.position(headerSize);

        while (buf.position() < header.lastOffset()) {
            int entryStart = buf.position();

            if (buf.remaining() < HEADER_SIZE) {
                throw new InvalidWavePatternException("Corrupted segment: insufficient space at " + entryStart);
            }

            byte flag = buf.get();

            byte[] idBytes = new byte[ID_SIZE];
            buf.get(idBytes);
            int len = buf.getInt();
            buf.getInt();

            if (flag == 0x00) {
                int skip = align(HEADER_SIZE
                        + WavePatternCodec.estimateSize(len, false));
                buf.position(entryStart + skip);
                continue;
            }

            if (len <= 0 || len > MAX_LENGTH) {
                throw new InvalidWavePatternException("Invalid pattern length at " + entryStart + ": " + len);
            }

            int patternSize = WavePatternCodec.estimateSize(len, false);
            if (buf.remaining() < patternSize) {
                throw new InvalidWavePatternException("Insufficient bytes at offset " + entryStart);
            }

            ByteBuffer patternSlice = buf.slice();
            patternSlice.limit(patternSize);
            WavePattern pattern = WavePatternCodec.readFrom(patternSlice, false);

            int skip = align(HEADER_SIZE + patternSize);
            buf.position(entryStart + skip);

            String id = bytesToHex(idBytes);
            latest.put(id, new PatternWithId(id, pattern, entryStart));
        }

        return new ArrayList<>(latest.values());
    }

    public WavePattern readAt(long offset) {
        return readWithId(offset).pattern();
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


    private static void verifyManifestVersion(Path path) {
        // no-op for now
    }


    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(ID_SIZE * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    static int inferChecksumLength(Path path) {
        final int FULL_HDR = 39;
        final int MIN_HDR  = 35;

        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(FULL_HDR).order(ByteOrder.LITTLE_ENDIAN);

            int read = 0;
            while (read < buf.capacity()) {
                int n = ch.read(buf, read);
                if (n <= 0) break;
                read += n;
            }

            if (read < MIN_HDR) {
                return 8;
            }

            buf.rewind();
            buf.getInt();
            buf.getInt();
            buf.getLong();
            buf.getInt();
            buf.getLong();

            if (read >= FULL_HDR) {
                long checksum = buf.getLong();
                byte flag     = buf.get();
                byte pad1     = buf.get();
                byte pad2     = buf.get();

                return 8;
            } else {
                buf.getInt();
                byte flag = buf.get();
                return 4;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to infer checksum length", e);
        }
    }

    @Override
    public void close() throws IOException {
        Buffers.unmap(mmap);
        channel.close();
    }
}