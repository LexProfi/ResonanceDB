/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.wal;

import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32C;

public final class WalRecord {

    public static final byte TYPE_INSERT = 1;
    public static final byte TYPE_DELETE = 2;
    public static final byte TYPE_REPLACE = 3;
    public static final byte TYPE_CHECKPOINT = 4;

    static final int RECORD_OVERHEAD = 17;

    private final long lsn;
    private final byte type;
    private final byte[] payload;

    private WalRecord(long lsn, byte type, byte[] payload) {
        this.lsn = lsn;
        this.type = type;
        this.payload = payload;
    }

    public long lsn() { return lsn; }
    public byte type() { return type; }
    public byte[] payload() { return payload; }

    public static WalRecord insert(long lsn, String contentHash,
                                   Map<String, String> metadata,
                                   WavePattern pattern) {
        byte[] payload = encodeInsertPayload(contentHash, metadata, pattern);
        return new WalRecord(lsn, TYPE_INSERT, payload);
    }

    public static WalRecord delete(long lsn, String contentHash) {
        byte[] hashBytes = contentHash.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + hashBytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort((short) hashBytes.length);
        buf.put(hashBytes);
        return new WalRecord(lsn, TYPE_DELETE, buf.array());
    }

    public static WalRecord replace(long lsn, String oldId, String newContentHash,
                                    Map<String, String> metadata,
                                    WavePattern newPattern) {
        byte[] oldIdBytes = oldId.getBytes(StandardCharsets.UTF_8);
        byte[] insertPayload = encodeInsertPayload(newContentHash, metadata, newPattern);
        ByteBuffer buf = ByteBuffer.allocate(2 + oldIdBytes.length + insertPayload.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort((short) oldIdBytes.length);
        buf.put(oldIdBytes);
        buf.put(insertPayload);
        return new WalRecord(lsn, TYPE_REPLACE, buf.array());
    }

    public static WalRecord checkpoint(long lsn, long maxSealedLsn) {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(maxSealedLsn);
        return new WalRecord(lsn, TYPE_CHECKPOINT, buf.array());
    }

    public byte[] toBytes() {
        int totalLen = RECORD_OVERHEAD + payload.length;
        ByteBuffer buf = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN);

        buf.putInt(totalLen);
        buf.putLong(lsn);
        buf.put(type);
        buf.put(payload);

        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 0, totalLen - 4);
        buf.putInt((int) crc.getValue());

        return buf.array();
    }

    public static WalRecord fromBuffer(ByteBuffer buf) throws WalCorruptionException {
        if (buf.remaining() < 4) return null;

        int startPos = buf.position();
        int totalLen = buf.getInt(startPos);

        if (totalLen < RECORD_OVERHEAD) {
            throw new WalCorruptionException(
                    "Invalid record length " + totalLen + " at position " + startPos);
        }
        if (buf.remaining() < totalLen) return null;

        byte[] recordBytes = new byte[totalLen];
        buf.get(recordBytes);

        CRC32C crc = new CRC32C();
        crc.update(recordBytes, 0, totalLen - 4);
        int expected = (int) crc.getValue();

        ByteBuffer rec = ByteBuffer.wrap(recordBytes).order(ByteOrder.LITTLE_ENDIAN);
        rec.position(totalLen - 4);
        int stored = rec.getInt();

        if (expected != stored) {
            buf.position(startPos);
            throw new WalCorruptionException(
                    "CRC mismatch at position " + startPos +
                    ": expected=0x" + Integer.toHexString(expected) +
                    ", stored=0x" + Integer.toHexString(stored));
        }

        rec.position(4);
        long lsn = rec.getLong();
        byte type = rec.get();
        int payloadLen = totalLen - RECORD_OVERHEAD;
        byte[] payload = new byte[payloadLen];
        rec.get(payload);

        return new WalRecord(lsn, type, payload);
    }

    public record InsertPayload(String contentHash, Map<String, String> metadata,
                                WavePattern pattern) {}

    public record ReplacePayload(String oldId, InsertPayload insert) {}

    public InsertPayload decodeInsert() {
        return decodeInsertPayload(ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN));
    }

    public String decodeDeleteId() {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        short len = buf.getShort();
        byte[] idBytes = new byte[len];
        buf.get(idBytes);
        return new String(idBytes, StandardCharsets.UTF_8);
    }

    public ReplacePayload decodeReplace() {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        short oldIdLen = buf.getShort();
        byte[] oldIdBytes = new byte[oldIdLen];
        buf.get(oldIdBytes);
        String oldId = new String(oldIdBytes, StandardCharsets.UTF_8);
        InsertPayload insert = decodeInsertPayload(buf);
        return new ReplacePayload(oldId, insert);
    }

    public long decodeCheckpointLsn() {
        return ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static byte[] encodeInsertPayload(String contentHash,
                                              Map<String, String> metadata,
                                              WavePattern pattern) {
        byte[] hashBytes = contentHash.getBytes(StandardCharsets.UTF_8);
        int patternLen = pattern.amplitude().length;

        Map<String, String> safeMeta = metadata != null ? metadata : Map.of();
        int metaSize = 2;
        for (var entry : safeMeta.entrySet()) {
            byte[] k = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = entry.getValue().getBytes(StandardCharsets.UTF_8);
            metaSize += 2 + k.length + 2 + v.length;
        }

        int totalPayload = 2 + hashBytes.length + metaSize + 4 + patternLen * 16;
        ByteBuffer buf = ByteBuffer.allocate(totalPayload).order(ByteOrder.LITTLE_ENDIAN);

        buf.putShort((short) hashBytes.length);
        buf.put(hashBytes);

        buf.putShort((short) safeMeta.size());
        for (var entry : safeMeta.entrySet()) {
            byte[] k = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = entry.getValue().getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) k.length);
            buf.put(k);
            buf.putShort((short) v.length);
            buf.put(v);
        }

        buf.putInt(patternLen);
        for (int i = 0; i < patternLen; i++) {
            buf.putDouble(pattern.amplitude()[i]);
        }
        for (int i = 0; i < patternLen; i++) {
            buf.putDouble(pattern.phase()[i]);
        }

        return buf.array();
    }

    private static InsertPayload decodeInsertPayload(ByteBuffer buf) {
        short hashLen = buf.getShort();
        byte[] hashBytes = new byte[hashLen];
        buf.get(hashBytes);
        String contentHash = new String(hashBytes, StandardCharsets.UTF_8);

        short metaCount = buf.getShort();
        Map<String, String> metadata = new HashMap<>();
        for (int i = 0; i < metaCount; i++) {
            short kLen = buf.getShort();
            byte[] k = new byte[kLen];
            buf.get(k);
            short vLen = buf.getShort();
            byte[] v = new byte[vLen];
            buf.get(v);
            metadata.put(new String(k, StandardCharsets.UTF_8),
                         new String(v, StandardCharsets.UTF_8));
        }

        int patternLen = buf.getInt();
        double[] amplitude = new double[patternLen];
        double[] phase = new double[patternLen];
        for (int i = 0; i < patternLen; i++) {
            amplitude[i] = buf.getDouble();
        }
        for (int i = 0; i < patternLen; i++) {
            phase[i] = buf.getDouble();
        }

        return new InsertPayload(contentHash, metadata, new WavePattern(amplitude, phase));
    }
}
