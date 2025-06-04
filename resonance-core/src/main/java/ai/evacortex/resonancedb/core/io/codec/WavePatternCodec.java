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

    private static final int MAGIC = 0x57565750; // 'WWWP'
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final int MAX_SUPPORTED_LENGTH = 8192; // architectural sanity limit

    /**
     * Serializes a WavePattern into the given ByteBuffer.
     *
     * Format:
     * if withMagic = true:
     *   [MAGIC:int][LENGTH:int][AMP₀:double]...[AMPₙ][PHASE₀:double]...[PHASEₙ]
     * else:
     *   [LENGTH:int][AMP₀:double]...[AMPₙ][PHASE₀:double]...[PHASEₙ]
     */
    public static void writeTo(ByteBuffer buf, WavePattern pattern, boolean withMagic) {
        double[] amp = pattern.amplitude();
        double[] phase = pattern.phase();
        int len = amp.length;

        if (pattern.amplitude().length != pattern.phase().length) {
            throw new IllegalArgumentException("Amplitude and phase arrays must be of equal length.");
        }

        if (len <= 0 || len > MAX_SUPPORTED_LENGTH) {
            throw new IllegalArgumentException("Unsupported WavePattern length: " + len);
        }

        buf.order(ORDER);
        if (withMagic) buf.putInt(MAGIC);
        buf.putInt(len);
        for (double v : amp) buf.putDouble(v);
        for (double v : phase) buf.putDouble(v);
    }

    /**
     * Reads a WavePattern from a ByteBuffer.
     * @param buf byte buffer with data
     * @param withMagic whether to expect a MAGIC header
     * @return reconstructed WavePattern
     */
    public static WavePattern readFrom(ByteBuffer buf, boolean withMagic) {
        buf.order(ORDER);

        if (withMagic) {
            if (buf.remaining() < 4) throw new IllegalArgumentException("No space for MAGIC");
            int magic = buf.getInt();
            if (magic != MAGIC) throw new IllegalArgumentException("Invalid MAGIC header");
        }

        if (buf.remaining() < 4) throw new IllegalArgumentException("No space for length header");
        int len = buf.getInt();

        if (len <= 0 || len > MAX_SUPPORTED_LENGTH) {
            throw new IllegalArgumentException("Suspicious pattern length: " + len);
        }

        int required = 8 * len * 2;
        if (buf.remaining() < required) {
            throw new IllegalArgumentException("Buffer underflow: need " + required + " bytes, found " + buf.remaining());
        }

        double[] amp = new double[len];
        double[] phase = new double[len];

        for (int i = 0; i < len; i++) amp[i] = buf.getDouble();
        for (int i = 0; i < len; i++) phase[i] = buf.getDouble();

        return new WavePattern(amp, phase);
    }

    /**
     * Serializes pattern with MAGIC header into byte array.
     */
    public static byte[] serialize(WavePattern pattern) {
        ByteBuffer buf = ByteBuffer.allocate(estimateSize(pattern, true)).order(ORDER);
        writeTo(buf, pattern, true);
        return buf.array();
    }

    /**
     * Deserializes pattern from byte array (expects MAGIC header).
     */
    public static WavePattern deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ORDER);
        return readFrom(buf, true);
    }

    /**
     * Estimate the number of bytes required to store a pattern.
     * @param pattern pattern to measure
     * @param withMagic include header size
     */
    public static int estimateSize(WavePattern pattern, boolean withMagic) {
        return estimateSize(pattern.amplitude().length, withMagic);
    }

    /**
     * Estimate the number of bytes required to store a pattern of given length.
     * @param length number of amplitudes (and phases)
     * @param withMagic include header size
     */
    public static int estimateSize(int length, boolean withMagic) {
        if (length <= 0 || length > MAX_SUPPORTED_LENGTH) {
            throw new IllegalArgumentException("Invalid length for size estimation: " + length);
        }
        int base = 4 + (8 * 2 * length);
        return withMagic ? base + 4 : base;
    }
}