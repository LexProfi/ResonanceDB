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
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;

import java.util.List;

/**
 * {@code NativeKernel} delegates resonance comparison of {@link WavePattern} instances
 * to a native implementation via Panama. This enables hardware-accelerated
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
 * @author Alexander Listopad
 * @see ResonanceKernel
 * @see NativeCompare
 */
public final class NativeKernel implements ResonanceKernel {

    @Override
    public float compare(WavePattern a, WavePattern b) {
        return compare(a, b, CompareOptions.defaultOptions());
    }

    @Override
    public float compare(WavePattern a, WavePattern b, CompareOptions options) {
        if (a == null || b == null) {
            throw new NullPointerException("WavePatterns must not be null");
        }

        if (options.ignorePhase()) {
            // fallback to JavaKernel since native ignorePhase is not implemented
            return new JavaKernel().compare(a, b, options);
        }

        try {
            float[] amp1 = toFloatArray(a.amplitude());
            float[] phase1 = toFloatArray(a.phase());
            float[] amp2 = toFloatArray(b.amplitude());
            float[] phase2 = toFloatArray(b.phase());

            return NativeCompare.compare(amp1, phase1, amp2, phase2);
        } catch (Throwable e) {
            throw new RuntimeException("Native comparison failed", e);
        }
    }

    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates) {
        return compareMany(query, candidates, CompareOptions.defaultOptions());
    }

    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates, CompareOptions options) {
        if (query == null || candidates == null) {
            throw new NullPointerException("Query and candidates must not be null");
        }

        if (options.ignorePhase()) {
            throw new UnsupportedOperationException("NativeKernel does not support ignorePhase yet");
        }

        try {
            float[] ampQ = toFloatArray(query.amplitude());
            float[] phaseQ = toFloatArray(query.phase());

            float[][] ampList = new float[candidates.size()][];
            float[][] phaseList = new float[candidates.size()][];

            for (int i = 0; i < candidates.size(); i++) {
                ampList[i] = toFloatArray(candidates.get(i).amplitude());
                phaseList[i] = toFloatArray(candidates.get(i).phase());
            }

            return NativeCompare.compareMany(ampQ, phaseQ, ampList, phaseList);
        } catch (Throwable e) {
            throw new RuntimeException("Native batch comparison failed", e);
        }
    }

    /**
     * Converts a {@code double[]} to a {@code float[]} via casting.
     *
     * @param input array of double values
     * @return array of float values
     */
    private float[] toFloatArray(double[] input) {
        float[] out = new float[input.length];
        for (int i = 0; i < input.length; i++) {
            out[i] = (float) input[i];
        }
        return out;
    }
}