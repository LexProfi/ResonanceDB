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

    public static final int CHECKSUM_4B = 4;
    public static final int CHECKSUM_8B = 8;

    private final int version;
    private final long timestamp;
    private final int recordCount;
    private final long lastOffset;
    private final long checksum;
    private final byte commitFlag;
    private final int checksumLength;

    public static int sizeFor(int checksumLength) {
        return switch (checksumLength) {
            case CHECKSUM_4B -> 32;
            case CHECKSUM_8B -> 36;
            default -> throw new IllegalArgumentException("Unsupported checksum length: " + checksumLength);
        };
    }

    public BinaryHeader(int version, long timestamp, int recordCount, long lastOffset,
                        long checksum, byte commitFlag, int checksumLength) {
        if (checksumLength != CHECKSUM_4B && checksumLength != CHECKSUM_8B)
            throw new IllegalArgumentException("Invalid checksum length: " + checksumLength);
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
        buf.putInt(version);
        buf.putLong(timestamp);
        buf.putInt(recordCount);
        buf.putLong(lastOffset);

        if (checksumLength == CHECKSUM_4B) {
            buf.putInt((int) checksum);
        } else if (checksumLength == CHECKSUM_8B) {
            buf.putLong(checksum);
        } else {
            throw new IllegalArgumentException("Unsupported checksum length: " + checksumLength);
        }

        buf.put(commitFlag);
        for (int i = 0; i < 3; i++) buf.put((byte) 0);

        return buf.array();
    }

    public static BinaryHeader from(ByteBuffer buf, int checksumLength) {
        buf.order(ByteOrder.LITTLE_ENDIAN);

        int version = buf.getInt();
        long timestamp = buf.getLong();
        int recordCount = buf.getInt();
        long lastOffset = buf.getLong();

        long checksum;
        byte commitFlag;

        if (checksumLength == CHECKSUM_4B) {
            checksum = buf.getInt() & 0xFFFFFFFFL;
            commitFlag = buf.get();
            buf.position(buf.position() + 3);
        }  else if (checksumLength == CHECKSUM_8B) {
            checksum = buf.getLong();
            commitFlag = buf.get();
            buf.position(buf.position() + 3);
        } else {
            throw new IllegalArgumentException("Invalid checksum length: " + checksumLength);
        }

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
