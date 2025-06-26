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
import java.util.*;
import java.util.stream.Stream;

public class CachedReader implements AutoCloseable {

    private static final int ID_SIZE = 16;
    private static final int HEADER_SIZE = 1 + 16 + 4 + 4;
    private static final int ALIGNMENT = 8;

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer mmap;
    private final Map<String, Long> offsetMap;
    private final long lastOffset;
    private final long weightInBytes;
    private volatile boolean closed = false;

    private CachedReader(Path path, FileChannel channel, MappedByteBuffer mmap,
                         Map<String, Long> offsetMap, long lastOffset, long weightInBytes) {
        this.path = path;
        this.channel = channel;
        this.mmap = mmap;
        this.offsetMap = offsetMap;
        this.lastOffset = lastOffset;
        this.weightInBytes = weightInBytes;
    }

    public static CachedReader open(Path segmentPath) throws IOException {
        FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ);
        long fileSize = channel.size();

        MappedByteBuffer mmap = (MappedByteBuffer) channel
                .map(FileChannel.MapMode.READ_ONLY, 0, fileSize)
                .order(ByteOrder.LITTLE_ENDIAN);

        int checksumLen = SegmentReader.inferChecksumLength(segmentPath);
        int hdrSize = BinaryHeader.sizeFor(checksumLen);

        ByteBuffer hdrBuf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        BinaryHeader header = BinaryHeader.from(hdrBuf, checksumLen);

        if (header.commitFlag() != 0 && header.commitFlag() != 1) {
            throw new IncompleteWriteException("Segment " + segmentPath.getFileName() +
                    " has unknown commit flag: " + header.commitFlag());
            }

        long lastOffset = header.lastOffset();
        Map<String, Long> offsetMap = new LinkedHashMap<>();

        int pos = hdrSize;
        while (pos + HEADER_SIZE <= lastOffset) {
            ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            buf.position(pos);

            byte deleted = buf.get();
            byte[] idBytes = new byte[ID_SIZE];
            buf.get(idBytes);
            int len = buf.getInt();
            buf.getInt();
            if (len <= 0 || len > WavePatternCodec.MAX_SUPPORTED_LENGTH) {
                break;
            }

            int patternSize = WavePatternCodec.estimateSize(len, false);
            int totalSize   = align(HEADER_SIZE + patternSize);

            if (deleted == 0x01) {
                String id = bytesToHex(idBytes);
                offsetMap.put(id, (long) pos);
            }
            pos += totalSize;
        }

        return new CachedReader(segmentPath, channel, mmap, offsetMap, lastOffset, fileSize);
    }

    public Set<String> allIds() {
        ensureOpen();
        return offsetMap.keySet();
    }

    public boolean contains(String id) {
        ensureOpen();
        return offsetMap.containsKey(id);
    }

    public long offsetOf(String id) {
        ensureOpen();
        Long off = offsetMap.get(id);
        if (off == null) throw new PatternNotFoundException("ID not found: " + id);
        return off;
    }

    public SegmentReader.PatternWithId readAtOffset(long offset) {
        ensureOpen();
        if (offset < 0 || offset + HEADER_SIZE >= lastOffset) {
            throw new InvalidWavePatternException("Offset out of bounds: " + offset);
        }
        ByteBuffer local = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        return SegmentReader.readWithIdFromBuffer(local, offset);
    }

    public WavePattern readById(String id) {
        ensureOpen();
        return readAtOffset(offsetOf(id)).pattern();
    }

    public Stream<SegmentReader.PatternWithId> lazyStream() {
        ensureOpen();
        return offsetMap.entrySet().stream().map(entry -> {
            try {
                return readAtOffset(entry.getValue());
            } catch (RuntimeException e) {
                return null;
            }
        }).filter(Objects::nonNull);
    }


    public Path getPath() {
        return path;
    }

    private static int align(int size) {
        return ((size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
    }

    public long getWeightInBytes() {
        return weightInBytes;
    }

    private static String bytesToHex(byte[] bytes) {
        final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2]     = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }


    void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Attempted to access closed CachedReader for " + path);
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            Buffers.unmap(mmap);
            channel.close();
        } catch (IOException ignored) {
        }
    }
}
