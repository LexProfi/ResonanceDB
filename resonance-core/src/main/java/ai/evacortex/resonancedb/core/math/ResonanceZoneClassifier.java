/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.math;

import ai.evacortex.resonancedb.core.ResonanceZone;

/**
 * Classifies resonance matches into semantic zones based on energy and phase alignment.
 */


public final class ResonanceZoneClassifier {

    private static final double SIGMA = Math.PI / 8;
    private static final float CORE_THRESHOLD = 0.85f;
    private static final float FRINGE_THRESHOLD = 0.3f;
    private static final double PHASE_LIMIT = Math.PI / 8;

    private static final int LUT_SIZE = 1000;
    private static final double[] SIGMOID_LUT = new double[LUT_SIZE + 1];
    private static final double[] GAUSS_LUT = new double[LUT_SIZE + 1];

    static {
        for (int i = 0; i <= LUT_SIZE; i++) {
            double x = i / (double) LUT_SIZE;
            double phi = x * Math.PI;
            SIGMOID_LUT[i] = 1.0 / (1.0 + Math.exp(-10.0 * (x - 0.5)));
            GAUSS_LUT[i] = Math.exp(-Math.pow(phi, 2) / (2 * Math.pow(SIGMA, 2)));
        }
    }

    private ResonanceZoneClassifier() {}

    public static ResonanceZone classify(float energy, double phaseShift) {
        double absPhase = Math.abs(phaseShift % (2 * Math.PI));
        if (energy >= CORE_THRESHOLD && absPhase <= PHASE_LIMIT) {
            return ResonanceZone.CORE;
        } else if (energy >= FRINGE_THRESHOLD) {
            return ResonanceZone.FRINGE;
        } else {
            return ResonanceZone.SHADOW;
        }
    }

    public static double computeScore(float energy, double phaseDelta) {
        double e = Math.max(0.0, Math.min(1.0, energy));
        double p = Math.max(0.0, Math.min(Math.PI, phaseDelta));
        int energyIndex = (int) Math.round(e * LUT_SIZE);
        int phaseIndex = (int) Math.round((p / Math.PI) * LUT_SIZE);
        return SIGMOID_LUT[energyIndex] * GAUSS_LUT[phaseIndex];
    }

    public static double meanPhaseDelta(Complex[] a, Complex[] b) {
        if (a.length != b.length) throw new IllegalArgumentException("Mismatched lengths");
        double total = 0.0;
        for (int i = 0; i < a.length; i++) {
            double delta = a[i].phase() - b[i].phase();
            total += Math.abs(Math.atan2(Math.sin(delta), Math.cos(delta))); // wrap to [–π, π]
        }
        return total / a.length;
    }

}