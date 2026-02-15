/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.io.codec;

import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

/**
 * Codec utility for encoding and decoding {@link WavePattern} objects into a binary format
 * suitable for durable storage and fast deserialization.
 *
 * <p>
 * This class supports optional inclusion of a format marker (magic header) and handles
 * conversion of fixed-length double-precision vectors (amplitude and phase) into
 * a compact binary representation.
 * </p>
 *
 * <h3>Encoding</h3>
 * Patterns can be written to or read from {@link ByteBuffer} containers. Serialization includes:
 * <ul>
 *   <li>Optional format marker (magic number)</li>
 *   <li>Pattern length</li>
 *   <li>Amplitude values</li>
 *   <li>Phase values</li>
 * </ul>
 * The layout is structured for forward compatibility and alignment with memory-mapped access.
 *
 * <h3>Format Detection</h3>
 * When enabled, a format identifier is prepended to help distinguish storage layouts
 * or versions in multi-source environments.
 *
 * <h3>Safety</h3>
 * Size constraints and sanity checks are enforced to prevent corruption or malicious input.
 * The codec is tolerant to various buffer origins, including heap and direct memory.
 *
 * <h3>Usage</h3>
 * This utility is typically used by storage-layer components such as {@link ./SegmentWriter}
 * and {@link ./SegmentReader}.
 *
 * <h3>Limits</h3>
 * Maximum supported vector length is bounded by an architectural constant and may
 * evolve in future versions without breaking compatibility.
 *
 * @see WavePattern
 * @see ./SegmentWriter
 * @see ./SegmentReader
 */
public class WavePatternCodec {

    private static final int MAGIC = 0x57565750; // 'WWWP'
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    public static final int MAX_SUPPORTED_LENGTH = 65_536; // architectural sanity limit

    public static void writeTo(ByteBuffer buf, WavePattern pattern, boolean withMagic) {
        double[] amp = pattern.amplitude();
        double[] phase = pattern.phase();
        int len = amp.length;

        if (pattern.amplitude().length != pattern.phase().length) {
            throw new InvalidWavePatternException("Amplitude and phase arrays must be of equal length.");
        }

        if (len <= 0 || len > MAX_SUPPORTED_LENGTH) {
            throw new InvalidWavePatternException("Unsupported WavePattern length: " + len);
        }

        buf.order(ORDER);
        if (withMagic) buf.putInt(MAGIC);
        buf.putInt(len);
        for (double v : amp) buf.putDouble(v);
        for (double v : phase) buf.putDouble(v);
    }

    public static void writeDirect(MappedByteBuffer mmap, WavePattern pattern) {
        double[] amp = pattern.amplitude();
        double[] phase = pattern.phase();
        int len = amp.length;
        if (len <= 0 || len > MAX_SUPPORTED_LENGTH) {
            throw new InvalidWavePatternException("Unsupported WavePattern length: " + len);
        }
        mmap.order(ORDER);
        mmap.putInt(len);
        for (double v : amp) mmap.putDouble(v);
        for (double v : phase) mmap.putDouble(v);
    }

    public static WavePattern readFrom(ByteBuffer buf, boolean withMagic) {
        buf.order(ORDER);

        if (withMagic) {
            if (buf.remaining() < 4) throw new InvalidWavePatternException("No space for MAGIC");
            int magic = buf.getInt();
            if (magic != MAGIC) throw new InvalidWavePatternException("Invalid MAGIC header");
        }

        if (buf.remaining() < 4) throw new InvalidWavePatternException("No space for length header");
        int len = buf.getInt();

        if (len <= 0 || len > MAX_SUPPORTED_LENGTH) {
            throw new InvalidWavePatternException("Suspicious pattern length: " + len);
        }

        int required = 8 * len * 2;
        if (buf.remaining() < required) {
            throw new InvalidWavePatternException("Buffer underflow: need " + required + " bytes, found "
                    + buf.remaining());
        }

        double[] amp = new double[len];
        double[] phase = new double[len];

        for (int i = 0; i < len; i++) amp[i] = buf.getDouble();
        for (int i = 0; i < len; i++) phase[i] = buf.getDouble();

        return new WavePattern(amp, phase);
    }

    public static byte[] serialize(WavePattern pattern) {
        int size = estimateSize(pattern, true);
        ByteBuffer buf = ByteBuffer.allocateDirect(size).order(ORDER);
        writeTo(buf, pattern, true);
        buf.flip();
        byte[] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }

    public static WavePattern deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ORDER);
        return readFrom(buf, true);
    }

    public static int estimateSize(WavePattern pattern, boolean withMagic) {
        return estimateSize(pattern.amplitude().length, withMagic);
    }

    public static int estimateSize(int length, boolean withMagic) {
        if (length <= 0 || length > MAX_SUPPORTED_LENGTH) {
            throw new InvalidWavePatternException("Invalid length for size estimation: " + length);
        }
        int base = 4 + (8 * 2 * length);
        return withMagic ? base + 4 : base;
    }
}