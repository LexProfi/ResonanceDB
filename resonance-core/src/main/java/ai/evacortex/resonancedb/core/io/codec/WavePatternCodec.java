/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.io.codec;

import ai.evacortex.resonancedb.core.WavePattern;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class WavePatternCodec {

    private static final int MAGIC = 0x57565750; // 'WAVP'
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    /**
     * Serializes the pattern into a new byte array (used in CLI/tools).
     */
    public static byte[] serialize(WavePattern pattern) {
        ByteBuffer buf = ByteBuffer.allocate(estimateSize(pattern)).order(ORDER);
        writeTo(buf, pattern);
        return buf.array();
    }

    /**
     * Deserializes a pattern from a byte array (used in CLI/tools).
     */
    public static WavePattern deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ORDER);
        return readFrom(buf);
    }

    /**
     * Writes the pattern to an existing ByteBuffer (used in mmap storage).
     */
    public static void writeTo(ByteBuffer buf, WavePattern pattern) {
        double[] amp = pattern.amplitude();
        double[] phase = pattern.phase();
        int len = amp.length;

        buf.putInt(MAGIC);
        buf.putInt(len);
        for (double v : amp) buf.putDouble(v);
        for (double v : phase) buf.putDouble(v);
    }

    /**
     * Reads a pattern from the current position of ByteBuffer.
     */
    public static WavePattern readFrom(ByteBuffer buf) {
        int magic = buf.getInt();
        if (magic != MAGIC) throw new IllegalArgumentException("Invalid pattern header");

        int len = buf.getInt();
        double[] amp = new double[len];
        double[] phase = new double[len];

        for (int i = 0; i < len; i++) amp[i] = buf.getDouble();
        for (int i = 0; i < len; i++) phase[i] = buf.getDouble();

        return new WavePattern(amp, phase);
    }

    /**
     * Estimates the byte size of the serialized pattern.
     */
    public static int estimateSize(WavePattern pattern) {
        return 4 + 4 + 8 * 2 * pattern.amplitude().length;
    }
}