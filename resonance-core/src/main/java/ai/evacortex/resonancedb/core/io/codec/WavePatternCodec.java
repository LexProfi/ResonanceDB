/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
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

    private static final int MAGIC = 0x57565750;
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    public static void writeTo(ByteBuffer buf, WavePattern pattern, boolean withMagic) {
        double[] amp = pattern.amplitude();
        double[] phase = pattern.phase();
        int len = amp.length;

        if (withMagic) buf.putInt(MAGIC);
        buf.putInt(len);
        for (double v : amp) buf.putDouble(v);
        for (double v : phase) buf.putDouble(v);
    }

    public static WavePattern readFrom(ByteBuffer buf, boolean withMagic) {
        if (withMagic) {
            int magic = buf.getInt();
            if (magic != MAGIC) throw new IllegalArgumentException("Invalid pattern header");
        }

        int len = buf.getInt();
        double[] amp = new double[len];
        double[] phase = new double[len];

        for (int i = 0; i < len; i++) amp[i] = buf.getDouble();
        for (int i = 0; i < len; i++) phase[i] = buf.getDouble();

        return new WavePattern(amp, phase);
    }

    public static byte[] serialize(WavePattern pattern) {
        ByteBuffer buf = ByteBuffer.allocate(estimateSize(pattern, true)).order(ORDER);
        writeTo(buf, pattern, true);
        return buf.array();
    }

    public static WavePattern deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ORDER);
        return readFrom(buf, true);
    }

    public static int estimateSize(WavePattern pattern, boolean withMagic) {
        int base = 4 * 2 + 8 * 2 * pattern.amplitude().length;
        return withMagic ? base + 4 : base;
    }

    public static int estimateSize(int length, boolean withMagic) {
        int base = 4 + 8 * 2 * length;
        return withMagic ? base + 4 : base;
    }
}