/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.nativeffi;

import ai.evacortex.resonancedb.core.WavePattern;
import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;

/**
 * {@code NativeKernel} delegates resonance comparison of {@link WavePattern} instances
 * to a native implementation via JNI or Panama. This enables hardware-accelerated
 * computation using SIMD or platform-specific optimizations.
 *
 * <p>The similarity score is computed externally and must conform to the standard
 * contract of the {@link ResonanceKernel}, producing values in the range [0.0 ... 1.0].</p>
 *
 * <p>Internally, this class transforms double-precision {@code amplitude} and {@code phase}
 * arrays into float arrays suitable for native comparison.</p>
 *
 * <p>Use cases include high-performance execution paths or CPU-specific tuning
 * of wave-based semantic matching.</p>
 *
 * @see ResonanceKernel
 * @see NativeCompare
 */
public class NativeKernel implements ResonanceKernel {

    /**
     * Compares two wave patterns using the native backend.
     *
     * @param a the first wave pattern
     * @param b the second wave pattern
     * @return similarity score ∈ [0.0 ... 1.0]
     * @throws IllegalArgumentException if any input is {@code null}
     * @throws RuntimeException if native code throws an error
     */
    @Override
    public float compare(WavePattern a, WavePattern b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("WavePatterns must not be null");
        }

        try {
            float[] amp1   = toFloatArray(a.amplitude());
            float[] phase1 = toFloatArray(a.phase());
            float[] amp2   = toFloatArray(b.amplitude());
            float[] phase2 = toFloatArray(b.phase());

            return NativeCompare.compare(amp1, phase1, amp2, phase2);
        } catch (Throwable e) {
            throw new RuntimeException("Native comparison failed", e);
        }
    }

    /**
     * Compares two wave patterns using the native backend.
     * <p>
     * {@link CompareOptions} are currently not supported and will be ignored.
     * This placeholder is provided to conform to the {@link ResonanceKernel} interface.
     * A future version may add native support for options like phase insensitivity.
     *
     * @param a       the first wave pattern
     * @param b       the second wave pattern
     * @param options comparison configuration (ignored)
     * @return similarity score ∈ [0.0 ... 1.0]
     * @throws IllegalArgumentException if any input is {@code null}
     */
    @Override
    public float compare(WavePattern a, WavePattern b, CompareOptions options) {
        return compare(a, b); // options are ignored for now
    }

    /**
     * Converts a double array to a float array.
     *
     * @param input the source array
     * @return a new float array with the same values
     */
    private float[] toFloatArray(double[] input) {
        float[] out = new float[input.length];
        for (int i = 0; i < input.length; i++) {
            out[i] = (float) input[i];
        }
        return out;
    }
}