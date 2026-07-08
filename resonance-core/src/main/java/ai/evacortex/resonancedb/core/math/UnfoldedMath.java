/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.math;

import ai.evacortex.resonancedb.core.storage.WavePattern;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;

import java.util.Objects;

public final class UnfoldedMath {

    private static final VectorSpecies<Float> F_SPECIES = FloatVector.SPECIES_PREFERRED;

    public static final double NORM_SQ_FLOOR = 1e-30;

    private UnfoldedMath() {}

    public static double[] unfold(WavePattern p) {
        Objects.requireNonNull(p, "pattern must not be null");
        double[] amp = p.amplitude();
        double[] phase = p.phase();
        int n = amp.length;
        if (n != phase.length) {
            throw new IllegalArgumentException("Amplitude and phase lengths must match");
        }

        double[] u = new double[2 * n];
        for (int i = 0; i < n; i++) {
            double a = amp[i];
            double phi = phase[i];
            u[i] = a * Math.cos(phi);
            u[n + i] = a * Math.sin(phi);
        }
        return u;
    }

    public static double energy(WavePattern p) {
        Objects.requireNonNull(p, "pattern must not be null");
        double[] amp = p.amplitude();
        double e = 0.0;
        for (double a : amp) {
            e += a * a;
        }
        return e;
    }

    private static final jdk.incubator.vector.VectorSpecies<Double> D_SPECIES =
            jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED;

    public static double dotUnfolded(double[] u1, double[] u2) {
        Objects.requireNonNull(u1, "u1 must not be null");
        Objects.requireNonNull(u2, "u2 must not be null");
        if (u1.length != u2.length) {
            throw new IllegalArgumentException(
                    "Vector dimensions must match: " + u1.length + " vs " + u2.length);
        }

        int step = D_SPECIES.length();
        var acc = jdk.incubator.vector.DoubleVector.zero(D_SPECIES);
        int i = 0;
        int limit = u1.length - (u1.length % step);
        for (; i < limit; i += step) {
            var va = jdk.incubator.vector.DoubleVector.fromArray(D_SPECIES, u1, i);
            var vb = jdk.incubator.vector.DoubleVector.fromArray(D_SPECIES, u2, i);
            acc = va.fma(vb, acc);
        }
        double sum = acc.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);
        for (; i < u1.length; i++) {
            sum += u1[i] * u2[i];
        }
        return sum;
    }

    public static double dotFused(WavePattern a, WavePattern b) {
        Objects.requireNonNull(a, "a must not be null");
        Objects.requireNonNull(b, "b must not be null");
        double[] aA = a.amplitude(), aP = a.phase();
        double[] bA = b.amplitude(), bP = b.phase();

        int n = aA.length;
        if (n != aP.length || n != bA.length || n != bP.length) {
            throw new IllegalArgumentException("Pattern dimensions must match");
        }

        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            sum += aA[i] * bA[i] * Math.cos(bP[i] - aP[i]);
        }
        return sum;
    }

    public static double dotFusedFlat(double[] queryAmp, double[] queryPhase,
                                      double[] candAmp, double[] candPhase, int len) {
        double sum = 0.0;
        for (int i = 0; i < len; i++) {
            sum += queryAmp[i] * candAmp[i] * Math.cos(candPhase[i] - queryPhase[i]);
        }
        return sum;
    }

    public static float scoreFromDot(double dot, double e1, double e2) {
        double denom = e1 + e2;
        if (denom == 0.0) return 0.0f;

        double base = 0.5 * (denom + 2.0 * dot) / denom;
        double ampF = (e1 > 0.0 && e2 > 0.0) ? 2.0 * Math.sqrt(e1 * e2) / denom : 0.0;
        return (float) (base * ampF);
    }

    public static double[] l2Normalize(double[] v) {
        double norm = 0.0;
        for (double x : v) {
            norm += x * x;
        }
        if (norm == 0.0) return v;
        norm = Math.sqrt(norm);
        for (int i = 0; i < v.length; i++) {
            v[i] /= norm;
        }
        return v;
    }

    public static float[] unfoldFloat32(WavePattern p) {
        double[] amp = p.amplitude();
        double[] phase = p.phase();
        int n = amp.length;
        float[] u = new float[2 * n];
        for (int i = 0; i < n; i++) {
            double a = amp[i];
            double phi = phase[i];
            u[i] = (float) (a * Math.cos(phi));
            u[n + i] = (float) (a * Math.sin(phi));
        }
        return u;
    }

    public static float energyFloat32(WavePattern p) {
        double[] amp = p.amplitude();
        float e = 0.0f;
        for (double a : amp) {
            e += (float) (a * a);
        }
        return e;
    }

    public static float dotFloat32(float[] a, float[] b, int bOff, int len) {
        int step = F_SPECIES.length();
        FloatVector acc = FloatVector.zero(F_SPECIES);
        int i = 0;

        int limit = len - (len % step);
        for (; i < limit; i += step) {
            FloatVector va = FloatVector.fromArray(F_SPECIES, a, i);
            FloatVector vb = FloatVector.fromArray(F_SPECIES, b, bOff + i);
            acc = va.fma(vb, acc);
        }

        float sum = acc.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);

        for (; i < len; i++) {
            sum += a[i] * b[bOff + i];
        }
        return sum;
    }

    public static float scoreFromDotFloat32(float dot, float e1, float e2) {
        float denom = e1 + e2;
        if (denom == 0.0f) return 0.0f;
        float base = 0.5f * (denom + 2.0f * dot) / denom;
        float ampF = (e1 > 0.0f && e2 > 0.0f) ? 2.0f * (float) Math.sqrt(e1 * e2) / denom : 0.0f;
        return base * ampF;
    }

    public static double l2DistanceSquared(double[] u1, double[] u2) {
        if (u1.length != u2.length) {
            throw new IllegalArgumentException(
                    "Vector dimensions must match: " + u1.length + " vs " + u2.length);
        }
        double sum = 0.0;
        for (int i = 0; i < u1.length; i++) {
            double d = u1[i] - u2[i];
            sum += d * d;
        }
        return sum;
    }
}
