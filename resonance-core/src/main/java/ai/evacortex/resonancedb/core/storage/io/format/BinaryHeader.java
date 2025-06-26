/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.io.format;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
/**
 * BinaryHeader encodes metadata for a segment file used in ResonanceDB.
 * It appears at the beginning of each segment and contains structural
 * information needed for pattern indexing, integrity validation, and commit tracking.
 *
 * <p>
 * The header supports a configurable checksum length (4 or 8 bytes), followed
 * by a commit flag and reserved padding to ensure alignment. The actual header size
 * is determined via {@link #sizeFor(int)}.
 * </p>
 *
 * <h3>Fields</h3>
 * The header contains:
 * <ul>
 *   <li>Format version and logical timestamp</li>
 *   <li>Record count and write offset</li>
 *   <li>Checksum value over the data section</li>
 *   <li>Commit flag indicating finalized state</li>
 * </ul>
 *
 * <h3>Serialization</h3>
 * The header is written in little-endian byte order. Its size is fixed per segment,
 * based on the selected checksum configuration. Padding is added for alignment.
 *
 * <h3>Usage</h3>
 * This structure is used both by {@link ./SegmentWriter} when writing new data
 * and by {@link ./SegmentReader} to validate and navigate existing segments.
 *
 * <h3>Safety</h3>
 * The design allows flexibility in checksum and format strategy without
 * locking the implementation to a specific hash algorithm or platform layout.
 *
 * @see ./SegmentWriter
 * @see ./SegmentReader
 */
public final class BinaryHeader {

    public static final int MAGIC = 0x5244534E; // ASCII: 'RDSN'
    public static final int MAGIC_LENGTH = 4;
    public static final int VERSION_LENGTH = 2;
    public static final int TIMESTAMP_LENGTH = 8;
    public static final int RECORD_COUNT_LENGTH = 4;
    public static final int LAST_OFFSET_LENGTH = 8;
    public static final int COMMIT_FLAG_LENGTH = 1;

    private final int version;
    private final long timestamp;
    private final int recordCount;
    private final long lastOffset;
    private final long checksum;
    private final byte commitFlag;
    private final int checksumLength;

    public static int sizeFor(int checksumLength) {
        int rawSize = MAGIC_LENGTH + VERSION_LENGTH + TIMESTAMP_LENGTH +
                RECORD_COUNT_LENGTH + LAST_OFFSET_LENGTH +
                checksumLength + COMMIT_FLAG_LENGTH;
        return (rawSize % 4 == 0) ? rawSize : rawSize + (4 - (rawSize % 4));
    }

    public BinaryHeader(int version, long timestamp, int recordCount, long lastOffset,
                        long checksum, byte commitFlag, int checksumLength) {
        if (checksumLength < 4 || checksumLength > 32)
            throw new IllegalArgumentException("Unsupported checksum length: " + checksumLength);
        this.version = version;
        this.timestamp = timestamp;
        this.recordCount = recordCount;
        this.lastOffset = lastOffset;
        this.checksum = checksum;
        this.commitFlag = commitFlag;
        this.checksumLength = checksumLength;
    }

    public byte[] toBytes() {
        int size = sizeFor(checksumLength);
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);

        buf.putInt(MAGIC);
        buf.putShort((short) version);
        buf.putLong(timestamp);
        buf.putInt(recordCount);
        buf.putLong(lastOffset);

        if (checksumLength == 4) {
            buf.putInt((int) checksum);
        } else if (checksumLength == 8) {
            buf.putLong(checksum);
        } else {
            byte[] hashBytes = new byte[checksumLength];
            ByteBuffer.wrap(hashBytes).order(ByteOrder.LITTLE_ENDIAN).putLong(checksum);
            buf.put(hashBytes);
        }
        buf.put(commitFlag);
        int written = MAGIC_LENGTH + VERSION_LENGTH + TIMESTAMP_LENGTH + RECORD_COUNT_LENGTH + LAST_OFFSET_LENGTH +
                checksumLength + COMMIT_FLAG_LENGTH;
        int padding = (4 - (written % 4)) % 4;
        for (int i = 0; i < padding; i++) buf.put((byte) 0);
        return buf.array();
    }

    public static BinaryHeader from(ByteBuffer buf, int checksumLength) {
        buf.order(ByteOrder.LITTLE_ENDIAN);

        int magic = buf.getInt();
        if (magic != MAGIC)
            throw new IllegalArgumentException("Invalid magic: " + Integer.toHexString(magic));

        int version = buf.getShort() & 0xFFFF;
        long timestamp = buf.getLong();
        int recordCount = buf.getInt();
        long lastOffset = buf.getLong();

        long checksum;
        if (checksumLength == 4) {
            checksum = buf.getInt() & 0xFFFFFFFFL;
        } else if (checksumLength == 8) {
            checksum = buf.getLong();
        } else {
            byte[] hashBytes = new byte[checksumLength];
            buf.get(hashBytes);
            ByteBuffer tmp = ByteBuffer.wrap(hashBytes).order(ByteOrder.LITTLE_ENDIAN);
            checksum = tmp.getLong(); // first 8 bytes only
        }

        byte commitFlag = buf.get();
        int readBytes = MAGIC_LENGTH + VERSION_LENGTH + TIMESTAMP_LENGTH +
                RECORD_COUNT_LENGTH + LAST_OFFSET_LENGTH +
                checksumLength + COMMIT_FLAG_LENGTH;
        int padding = (4 - (readBytes % 4)) % 4;
        buf.position(buf.position() + padding);

        return new BinaryHeader(version, timestamp, recordCount, lastOffset, checksum, commitFlag, checksumLength);
    }

    public int version()        { return version; }
    public long timestamp()     { return timestamp; }
    public int recordCount()    { return recordCount; }
    public long lastOffset()    { return lastOffset; }
    public long checksum()      { return checksum; }
    public byte commitFlag()    { return commitFlag; }
    public int checksumLength() { return checksumLength; }
}