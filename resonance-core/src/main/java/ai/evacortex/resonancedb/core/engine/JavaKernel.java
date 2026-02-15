/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public final class JavaKernel implements ResonanceKernel {

    @Override
    public float compare(WavePattern a, WavePattern b) {
        return compare(a, b, CompareOptions.defaultOptions());
    }

    @Override
    public float compare(WavePattern a, WavePattern b, CompareOptions options) {
        Objects.requireNonNull(a, "a must not be null");
        Objects.requireNonNull(b, "b must not be null");
        Objects.requireNonNull(options, "options must not be null");

        double[] aA = a.amplitude(), aP = a.phase();
        double[] bA = b.amplitude(), bP = b.phase();

        final int len = aA.length;
        if (len != aP.length || len != bA.length || len != bP.length) {
            throw new IllegalArgumentException("Mismatched lengths");
        }

        double eA = 0.0, eB = 0.0, inter = 0.0;

        if (options.ignorePhase()) {
            for (int i = 0; i < len; i++) {
                double A1 = aA[i], A2 = bA[i];
                double A1sq = A1 * A1, A2sq = A2 * A2;
                eA += A1sq;
                eB += A2sq;
                inter += (A1 + A2) * (A1 + A2);
            }
        } else {
            for (int i = 0; i < len; i++) {
                double A1 = aA[i], A2 = bA[i];
                double A1sq = A1 * A1, A2sq = A2 * A2;
                eA += A1sq;
                eB += A2sq;
                double dphi = bP[i] - aP[i];
                inter += A1sq + A2sq + 2.0 * A1 * A2 * Math.cos(dphi);
            }
        }

        double denom = eA + eB;
        if (denom == 0.0) return 0.0f;

        double base = 0.5 * inter / denom;
        double ampF = (eA > 0.0 && eB > 0.0) ? 2.0 * Math.sqrt(eA * eB) / denom : 0.0;
        return (float) (base * ampF);
    }

    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates) {
        return compareMany(query, candidates, CompareOptions.defaultOptions());
    }

    @Override
    public float[] compareMany(WavePattern q, List<WavePattern> cands, CompareOptions options) {
        Objects.requireNonNull(q, "query must not be null");
        Objects.requireNonNull(cands, "candidates must not be null");
        Objects.requireNonNull(options, "options must not be null");

        final double[] qA = q.amplitude(), qP = q.phase();
        final int len = qA.length;
        if (len != qP.length) throw new IllegalArgumentException("Query length mismatch");

        for (int i = 0, n = cands.size(); i < n; i++) {
            WavePattern b = Objects.requireNonNull(cands.get(i), "candidate[" + i + "] is null");
            double[] bA = b.amplitude(), bP = b.phase();
            if (bA.length != len || bP.length != len) {
                throw new IllegalArgumentException("Candidate length mismatch at index " + i);
            }
        }

        final boolean ignorePhase = options.ignorePhase();
        final int n = cands.size();
        final float[] out = new float[n];

        IntStream.range(0, n).forEach(i -> {
            WavePattern b = cands.get(i);
            double[] bA = b.amplitude(), bP = b.phase();

            double eA = 0.0, eB = 0.0, inter = 0.0;

            if (ignorePhase) {
                for (int k = 0; k < len; k++) {
                    double A1 = qA[k], A2 = bA[k];
                    double A1sq = A1 * A1, A2sq = A2 * A2;
                    eA += A1sq; eB += A2sq;
                    inter += (A1 + A2) * (A1 + A2);
                }
            } else {
                for (int k = 0; k < len; k++) {
                    double A1 = qA[k], A2 = bA[k];
                    double A1sq = A1 * A1, A2sq = A2 * A2;
                    eA += A1sq; eB += A2sq;
                    inter += A1sq + A2sq + 2.0 * A1 * A2 * Math.cos(bP[k] - qP[k]);
                }
            }

            double denom = eA + eB;
            out[i] = (denom == 0.0) ? 0.0f
                    : (float) ((0.5 * inter / denom) *
                    ((eA > 0.0 && eB > 0.0) ? (2.0 * Math.sqrt(eA * eB) / denom) : 0.0));
        });

        return out;
    }

    @Override
    public ComparisonResult compareWithPhaseDelta(WavePattern a, WavePattern b) {
        Objects.requireNonNull(a, "a must not be null");
        Objects.requireNonNull(b, "b must not be null");

        double[] aA = a.amplitude(), aP = a.phase();
        double[] bA = b.amplitude(), bP = b.phase();
        final int len = aA.length;
        if (len != aP.length || len != bA.length || len != bP.length) {
            throw new IllegalArgumentException("Pattern length mismatch");
        }

        double eA = 0.0, eB = 0.0, inter = 0.0, dSum = 0.0;

        for (int i = 0; i < len; i++) {
            double A1 = aA[i], A2 = bA[i];
            double A1sq = A1 * A1, A2sq = A2 * A2;
            eA += A1sq; eB += A2sq;

            double dphi = bP[i] - aP[i];
            inter += A1sq + A2sq + 2.0 * A1 * A2 * Math.cos(dphi);

            while (dphi <= -Math.PI) dphi += 2 * Math.PI;
            while (dphi >  Math.PI) dphi -= 2 * Math.PI;
            dSum += dphi;
        }

        double denom = eA + eB;
        float energy = (denom == 0.0) ? 0.0f : (float) (0.5 * inter / denom);
        return new ComparisonResult(energy, dSum / len);
    }
}