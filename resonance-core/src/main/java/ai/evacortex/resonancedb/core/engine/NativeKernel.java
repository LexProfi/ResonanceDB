/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.util.List;
import java.util.Objects;


public final class NativeKernel implements ResonanceKernel {

    private static final JavaKernel JAVA_FALLBACK = new JavaKernel();
    private static final int CFG_BATCH = Math.max(1024, Integer.getInteger("resonance.native.batch", 8192));

    private static final ThreadLocal<float[]> TL_Q_AMP   = ThreadLocal.withInitial(() -> new float[0]);
    private static final ThreadLocal<float[]> TL_Q_PHASE = ThreadLocal.withInitial(() -> new float[0]);
    private static final ThreadLocal<float[]> TL_FLAT_AMP   = ThreadLocal.withInitial(() -> new float[0]);
    private static final ThreadLocal<float[]> TL_FLAT_PHASE = ThreadLocal.withInitial(() -> new float[0]);

    @Override
    public float compare(WavePattern a, WavePattern b) {
        return compare(a, b, CompareOptions.defaultOptions());
    }

    @Override
    public float compare(WavePattern a, WavePattern b, CompareOptions options) {
        Objects.requireNonNull(a, "WavePatterns must not be null");
        Objects.requireNonNull(b, "WavePatterns must not be null");
        Objects.requireNonNull(options, "CompareOptions must not be null");

        if (a.amplitude().length != a.phase().length || b.amplitude().length != b.phase().length) {
            throw new IllegalArgumentException("Amplitude/phase length mismatch");
        }

        if (options.ignorePhase()) {
            return JAVA_FALLBACK.compare(a, b, options);
        }

        final int len = a.amplitude().length;
        try {
            float[] amp1   = ensureAndFill(TL_Q_AMP,   a.amplitude(), len);
            float[] phase1 = ensureAndFill(TL_Q_PHASE, a.phase(),     len);

            float[] amp2   = new float[len];
            float[] phase2 = new float[len];
            castCopy(b.amplitude(), amp2,  0, len);
            castCopy(b.phase(),     phase2, 0, len);

            return NativeCompare.compare(amp1, phase1, amp2, phase2);
        } catch (Throwable e) {
            return JAVA_FALLBACK.compare(a, b, options);
        }
    }

    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates) {
        return compareMany(query, candidates, CompareOptions.defaultOptions());
    }

    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates, CompareOptions options) {
        Objects.requireNonNull(query, "Query pattern must not be null");
        Objects.requireNonNull(candidates, "Candidates list must not be null");
        Objects.requireNonNull(options, "CompareOptions must not be null");

        if (candidates.isEmpty()) return new float[0];
        if (options.ignorePhase()) {
            return JAVA_FALLBACK.compareMany(query, candidates, options);
        }

        final int len = query.amplitude().length;
        final int count = candidates.size();

        final boolean flatCompatible = candidates.stream()
                .allMatch(c -> c.amplitude().length == len && c.phase().length == len);

        try {
            final float[] ampQ   = ensureAndFill(TL_Q_AMP,   query.amplitude(), len);
            final float[] phaseQ = ensureAndFill(TL_Q_PHASE, query.phase(),     len);

            if (flatCompatible) {
                final int need = len * count;
                final float[] ampAll   = ensureCapacity(TL_FLAT_AMP,   need);
                final float[] phaseAll = ensureCapacity(TL_FLAT_PHASE, need);
                int dstOff = 0;
                for (int i = 0; i < count; i++) {
                    WavePattern c = candidates.get(i);
                    castCopy(c.amplitude(), ampAll,   dstOff, len);
                    castCopy(c.phase(),     phaseAll, dstOff, len);
                    dstOff += len;
                }
                return NativeCompare.compareManyFlat(ampQ, phaseQ, ampAll, phaseAll, len, count);
            }

            final float[] out = new float[count];
            final int batchSize = CFG_BATCH;

            for (int start = 0; start < count; start += batchSize) {
                final int n = Math.min(batchSize, count - start);
                final float[][] ampArr = new float[n][];
                final float[][] phaseArr = new float[n][];
                for (int i = 0; i < n; i++) {
                    WavePattern c = candidates.get(start + i);
                    float[] a = new float[len];
                    float[] p = new float[len];
                    castCopy(c.amplitude(), a, 0, len);
                    castCopy(c.phase(),     p, 0, len);
                    ampArr[i] = a;
                    phaseArr[i] = p;
                }

                float[] part = NativeCompare.compareMany(ampQ, phaseQ, ampArr, phaseArr);
                System.arraycopy(part, 0, out, start, n);
            }
            return out;

        } catch (Throwable e) {
            return JAVA_FALLBACK.compareMany(query, candidates, options);
        }
    }

    @Override
    public ComparisonResult compareWithPhaseDelta(WavePattern a, WavePattern b) {
        Objects.requireNonNull(a, "First pattern must not be null");
        Objects.requireNonNull(b, "Second pattern must not be null");

        if (a.amplitude().length != a.phase().length ||
                b.amplitude().length != b.phase().length ||
                a.amplitude().length != b.amplitude().length) {
            throw new IllegalArgumentException("Pattern length mismatch");
        }

        final int len = a.amplitude().length;
        try {
            float[] amp1   = ensureAndFill(TL_Q_AMP,   a.amplitude(), len);
            float[] phase1 = ensureAndFill(TL_Q_PHASE, a.phase(),     len);
            float[] amp2   = new float[len];
            float[] phase2 = new float[len];
            castCopy(b.amplitude(), amp2, 0, len);
            castCopy(b.phase(),     phase2, 0, len);

            float[] out = NativeCompare.compareWithPhaseDelta(amp1, phase1, amp2, phase2);
            return new ComparisonResult(out[0], out[1]);
        } catch (Throwable e) {
            return JAVA_FALLBACK.compareWithPhaseDelta(a, b);
        }
    }

    private static float[] ensureCapacity(ThreadLocal<float[]> tl, int need) {
        float[] a = tl.get();
        if (a.length < need) {
            int cap = Math.max(need, a.length * 2 + 1024);
            a = new float[cap];
            tl.set(a);
        }
        return a;
    }

    private static float[] ensureAndFill(ThreadLocal<float[]> tl, double[] src, int len) {
        float[] dst = ensureCapacity(tl, len);
        castCopy(src, dst, 0, len);
        return dst;
    }

    private static void castCopy(double[] src, float[] dst, int dstOff, int len) {
        for (int i = 0; i < len; i++) {
            dst[dstOff + i] = (float) src[i];
        }
    }
}
