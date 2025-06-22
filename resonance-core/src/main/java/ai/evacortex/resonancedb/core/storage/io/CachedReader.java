package ai.evacortex.resonancedb.core.storage.io;

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

public class CachedReader implements AutoCloseable {

    private static final int ID_SIZE = 16;
    private static final int HEADER_SIZE = 1 + 16 + 4 + 4; // id[16] + deleted[1] + len[4] + reserved[4]
    private static final int ALIGNMENT = 8;

    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer mmap;
    private final Map<String, Long> offsetMap;

    private CachedReader(Path path, FileChannel channel, MappedByteBuffer mmap, Map<String, Long> offsetMap) {
        this.path = path;
        this.channel = channel;
        this.mmap = mmap;
        this.offsetMap = offsetMap;
    }

    public static CachedReader open(Path segmentPath) throws IOException {
        // === mmap whole segment ==================================================
        FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ);
        long fileSize = channel.size();

        MappedByteBuffer mmap = (MappedByteBuffer) channel
                .map(FileChannel.MapMode.READ_ONLY, 0, fileSize)
                .order(ByteOrder.LITTLE_ENDIAN);

        /*--- определяем фактическую длину BinaryHeader (4-or-8-byte checksum) ---*/
        int checksumLen  = SegmentReader.inferChecksumLength(segmentPath); // уже есть util
        int fileHdrSize  = BinaryHeader.sizeFor(checksumLen);

        Map<String, Long> offsetMap = new LinkedHashMap<>();
        int pos = fileHdrSize;
        while (pos + HEADER_SIZE <= fileSize) {
            ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            buf.position(pos);

            byte deleted      = buf.get();          // 1 byte   — 0x01 = live, 0x00 = tombstone
            byte[] idBytes    = new byte[ID_SIZE];  // 16 bytes — MD5
            buf.get(idBytes);
            int len           = buf.getInt();       // 4 bytes  — amplitude length
            buf.getInt();                           // 4 bytes  — reserved

            // ───── live entry ─────
            if (deleted == 0x01 && len > 0 && len <= WavePatternCodec.MAX_SUPPORTED_LENGTH) {
                int patternSize = WavePatternCodec.estimateSize(len, false);
                int totalSize   = align(HEADER_SIZE + patternSize);

                String id = bytesToHex(idBytes);
                offsetMap.put(id, (long) pos);

                pos += totalSize;                   // advance to next header
                continue;
            }

            // ───── tombstone / garbage ─────
            if (len <= 0 || len > WavePatternCodec.MAX_SUPPORTED_LENGTH) {
                // corrupt length ⇒ stop scan (rest of file unusable)
                break;
            }

            int skip = align(HEADER_SIZE + WavePatternCodec.estimateSize(len, false));
            pos += skip;                            // skip over tombstoned record
        }

        return new CachedReader(segmentPath, channel, mmap, offsetMap);
    }

    public Set<String> allIds() {
        return offsetMap.keySet();
    }

    public boolean contains(String id) {
        return offsetMap.containsKey(id);
    }

    public long offsetOf(String id) {
        Long off = offsetMap.get(id);
        if (off == null) throw new PatternNotFoundException("ID not found: " + id);
        return off;
    }

    public WavePattern readAtOffset(long offset) {
        if (offset < 0 || offset + HEADER_SIZE >= mmap.capacity()) {
            throw new InvalidWavePatternException("Offset out of bounds: " + offset);
        }

        ByteBuffer buf = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        buf.position((int) offset);

        byte deleted = buf.get();                       // 0x01 = live, 0x00 = tombstone
        byte[] idBytes = new byte[ID_SIZE];
        buf.get(idBytes);
        int len = buf.getInt();
        buf.getInt();                                   // reserved

        if (deleted == 0x00) {                          // ← ПРАВИЛЬНАЯ проверка
            throw new PatternNotFoundException("Deleted pattern at offset " + offset);
        }

        int patternSize = WavePatternCodec.estimateSize(len, false);
        if (buf.remaining() < patternSize) {
            throw new InvalidWavePatternException("Insufficient bytes to decode pattern at offset " + offset);
        }

        ByteBuffer patternBuf = buf.slice();
        patternBuf.limit(patternSize);
        return WavePatternCodec.readFrom(patternBuf, false);
    }

    public WavePattern readById(String id) {
        return readAtOffset(offsetOf(id));
    }

    public Path getPath() {
        return path;
    }

    public List<SegmentReader.PatternWithId> readAllWithId() {
        List<SegmentReader.PatternWithId> result = new ArrayList<>();

        for (Map.Entry<String, Long> entry : offsetMap.entrySet()) {
            String id = entry.getKey();
            long offset = entry.getValue();
            try {
                WavePattern pattern = readAtOffset(offset);
                result.add(new SegmentReader.PatternWithId(id, pattern, offset));
            } catch (RuntimeException ex) {
                // Повреждённые или удалённые пропускаем
            }
        }

        return result;
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            // optional: log
        }
    }

    private static int align(int size) {
        return ((size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
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
}