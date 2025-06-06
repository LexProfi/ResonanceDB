/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.math.Complex;

public record WavePattern(double[] amplitude, double[] phase) {
    public Complex[] toComplex() {
        int len = amplitude.length;
        Complex[] spectrum = new Complex[len];
        for (int i = 0; i < len; i++) {
            double real = amplitude[i] * Math.cos(phase[i]);
            double imag = amplitude[i] * Math.sin(phase[i]);
            spectrum[i] = new Complex(real, imag);
        }
        return spectrum;
    }
}