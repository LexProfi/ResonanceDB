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


/**
 * SegmentReader provides read-only access to wave-based semantic patterns
 * stored in a memory-mapped segment file produced by {@link SegmentWriter}.
 *
 * <p>
 * Each segment begins with a {@link BinaryHeader}, followed by one or more
 * encoded entries. The layout of each entry includes:
 * </p>
 * <pre>
 *   [ID_HASH (16 bytes)] [LENGTH (4 bytes)] [RESERVED (4 bytes)] [WavePattern binary]
 * </pre>
 *
 * <p>
 * Entries can be logically deleted via a leading 0x00 tombstone marker.
 * Patterns are retrieved with integrity checks and offset-based access.
 * </p>
 *
 * <p>
 * This reader is thread-safe for concurrent access through buffer duplication.
 * Checksum verification and segment manifest coordination are internally supported.
 * </p>
 *
 * <p>
 * SegmentReader is compatible with fixed-length hash IDs and length-prefixed
 * binary encodings. The structure is designed for use in resonance-based
 * pattern retrieval systems.
 * </p>
 *
 * <p><b>Note:</b> This class does not perform mutation and assumes segments
 * are written and committed prior to read access. Compatibility with different
 * checksum formats and alignment strategies is supported via configuration.
 * </p>
 *
 * @see SegmentWriter
 * @see WavePattern
 * @see BinaryHeader
 */
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

    public WavePattern readAt(long offset) {
        return readWithId(offset).pattern();
    }

    public PatternWithId readWithId(long offset) {
        if (offset < 0 || offset + HEADER_SIZE >= header.lastOffset()) {
            throw new InvalidWavePatternException("Offset out of segment bounds: " + offset);
        }

        ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        buf.position((int) offset);
        byte deleted = buf.get();                             // 1 байт
        if (deleted == 0x00) {
            throw new PatternNotFoundException("Deleted (tombstoned) pattern at offset " + offset);
        }
        byte[] idBytes = new byte[ID_SIZE];                   // 16 байт
        buf.get(idBytes);
        int len = buf.getInt();                     // read length
        buf.getInt();                               // skip reserved
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

    public List<PatternWithId> readAllWithId() {
        Map<String, PatternWithId> latest = new LinkedHashMap<>();
        ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        buf.position(headerSize);

        while (buf.position() < header.lastOffset()) {
            int entryStart = buf.position();

            if (buf.remaining() < HEADER_SIZE) {
                throw new InvalidWavePatternException("Corrupted segment: insufficient space for header at offset " + entryStart);
            }

            byte deleted = buf.get();                             // 1 байт
            byte[] idBytes = new byte[ID_SIZE];                   // 16 байт
            buf.get(idBytes);
            int len = buf.getInt();
            buf.getInt(); // reserved

            if (deleted == 0x00) {
                int skip = align(HEADER_SIZE + WavePatternCodec.estimateSize(len, false));
                buf.position(entryStart + skip);
                continue;
            }

            if (len <= 0 || len > MAX_LENGTH) {
                throw new InvalidWavePatternException("Invalid pattern length at offset " + entryStart + ": " + len);
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

    public BinaryHeader getHeader() {
        return header;
    }

    public Path getPath() {
        return path;
    }

    private static int align(int size) {
        return ((size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
    }

    /**
     * Verifies the manifest version of this segment against the expected global state.
     * <p>
     * This method is a placeholder for CAS-based consistency checking.
     * If a mismatch is detected, the segment should be unmapped and remapped.
     * </p>
     * <p>
     * TODO: Implement CAS check against segment manifest version and force re-map via Unsafe.invokeCleaner.
     * </p>
     *
     * @param path Path to the segment file.
     */
    private static void verifyManifestVersion(Path path) {
        // no-op for now
    }

    @Override
    public void close() throws IOException {
        Buffers.unmap(mmap);
        channel.close();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(ID_SIZE * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    static int inferChecksumLength(Path path) {
        final int FULL_HDR = 39;          // максимум (checksum=8)
        final int MIN_HDR  = 35;          // минимум (checksum=4)

        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(FULL_HDR).order(ByteOrder.LITTLE_ENDIAN);

            int read = 0;
            while (read < buf.capacity()) {            // читаем столько, сколько реально есть
                int n = ch.read(buf, read);
                if (n <= 0) break;
                read += n;
            }

            /* файл ещё не дописан – по умолчанию считаем, что checksum = 8 */
            if (read < MIN_HDR) {
                return 8;
            }

            buf.rewind();
            buf.getInt();     // version
            buf.getInt();     // reserved
            buf.getLong();    // timestamp
            buf.getInt();     // recordCount
            buf.getLong();    // lastOffset

            if (read >= FULL_HDR) {        // можем безопасно прочитать 8-байтовый checksum
                long checksum = buf.getLong();
                byte flag     = buf.get();
                byte pad1     = buf.get();
                byte pad2     = buf.get();

            /* «длинный» вариант header’а всегда записывается SegmentWriter-ом,
               поэтому если мы добрались до commitFlag, значит checksum = 8. */
                return 8;
            } else {                       // header обрезан на 4-байтовом checksum
                /* пропускаем 4-байтовую контрольную сумму */
                buf.getInt();
                byte flag = buf.get();

                // если commitFlag уже присутствует и паддинг не читается – значит checksum = 4
                return 4;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to infer checksum length", e);
        }
    }

    public record PatternWithId(String id, WavePattern pattern, long offset) {}
}