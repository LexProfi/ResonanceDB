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
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;

public class NativeKernel implements ResonanceKernel {

    @Override
    public float compare(WavePattern a, WavePattern b) {

        if (a == null || b == null) throw new IllegalArgumentException("WavePatterns must not be null");

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

    private float[] toFloatArray(double[] input) {
        float[] out = new float[input.length];
        for (int i = 0; i < input.length; i++) {
            out[i] = (float) input[i];
        }
        return out;
    }
}
