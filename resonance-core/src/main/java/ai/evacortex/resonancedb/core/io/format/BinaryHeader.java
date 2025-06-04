/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.io.format;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public record BinaryHeader(int version, long timestamp, int recordCount, long lastOffset) {
    public static final int SIZE = 4 + 8 + 4 + 8; // version + timestamp + recordCount + lastOffset

    /**
     * Serializes header to byte array in little-endian format.
     */
    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(version);
        buf.putLong(timestamp);
        buf.putInt(recordCount);
        buf.putLong(lastOffset);
        return buf.array();
    }

    /**
     * Deserializes header from buffer in little-endian format.
     */
    public static BinaryHeader from(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int version = buf.getInt();
        long timestamp = buf.getLong();
        int recordCount = buf.getInt();
        long lastOffset = buf.getLong();
        return new BinaryHeader(version, timestamp, recordCount, lastOffset);
    }
}