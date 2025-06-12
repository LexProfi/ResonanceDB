/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.math;

import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class WavePatternUtils {

    private WavePatternUtils() {}

    public static WavePattern superpose(List<WavePattern> patterns, List<Double> weights) {
        Objects.requireNonNull(patterns, "patterns must not be null");

        if (patterns.isEmpty()) {
            throw new InvalidWavePatternException("At least one WavePattern required for superposition.");
        }

        int len = patterns.getFirst().amplitude().length;
        for (WavePattern p : patterns) {
            if (p.amplitude().length != len || p.phase().length != len) {
                throw new InvalidWavePatternException("All WavePatterns must have the same length.");
            }
        }

        boolean hasWeights = weights != null && !weights.isEmpty();
        if (hasWeights && weights.size() != patterns.size()) {
            throw new IllegalArgumentException("weights.size() must match patterns.size()");
        }

        Complex[] result = new Complex[len];
        Arrays.fill(result, new Complex(0.0, 0.0));

        for (int p = 0; p < patterns.size(); p++) {
            WavePattern wp = patterns.get(p);
            double w = hasWeights ? weights.get(p) : 1.0;
            Complex[] psi = wp.toComplex();

            for (int i = 0; i < len; i++) {
                result[i] = result[i].add(psi[i].scale(w));
            }
        }

        double[] amp = new double[len];
        double[] phase = new double[len];

        for (int i = 0; i < len; i++) {
            amp[i] = result[i].abs();
            phase[i] = result[i].phase();
        }

        return new WavePattern(amp, phase);
    }
}