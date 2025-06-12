/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.nativeffi;

import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;

import java.util.List;

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

    private float[] toFloatArray(double[] input) {
        float[] out = new float[input.length];
        for (int i = 0; i < input.length; i++) {
            out[i] = (float) input[i];
        }
        return out;
    }

    @Override
    public ComparisonResult compareWithPhaseDelta(WavePattern a, WavePattern b) {
        if (a == null || b == null) {
            throw new NullPointerException("Patterns must not be null");
        }
        if (a.amplitude().length != b.amplitude().length) {
            throw new IllegalArgumentException("Pattern length mismatch");
        }

        float[] amp1 = toFloatArray(a.amplitude());
        float[] phase1 = toFloatArray(a.phase());
        float[] amp2 = toFloatArray(b.amplitude());
        float[] phase2 = toFloatArray(b.phase());

        try {
            float[] out = NativeCompare.compareWithPhaseDelta(amp1, phase1, amp2, phase2);
            return new ComparisonResult(out[0], out[1]);
        } catch (Throwable e) {
            throw new RuntimeException("Native compareWithPhaseDelta failed", e);
        }
    }
}