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

public record BinaryHeader(int version, long timestamp, int length) {
    public static final int SIZE = 4 + 8 + 4;

    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(version);
        buf.putLong(timestamp);
        buf.putInt(length);
        return buf.array();
    }

    public static BinaryHeader from(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return new BinaryHeader(buf.getInt(), buf.getLong(), buf.getInt());
    }
}