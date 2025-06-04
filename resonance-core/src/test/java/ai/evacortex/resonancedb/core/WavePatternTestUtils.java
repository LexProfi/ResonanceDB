/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core;

import java.util.Arrays;


/**
 * Utility class for creating synthetic {@link WavePattern} instances for testing.
 */
public class WavePatternTestUtils {

    /**
     * Returns a fixed test pattern with hardcoded amplitude and phase values.
     */
    public static WavePattern fixedPattern() {
        double[] amp = {0.37454012, 0.9507143, 0.7319939, 0.5986584};
        double[] phase = {0.9802940, 0.9801420, 0.3649500, 5.4423451};
        return new WavePattern(amp, phase);
    }

    /**
     * Creates a WavePattern with constant amplitude and phase.
     *
     * @param amplitude constant amplitude value to fill
     * @param phase     constant phase value to fill
     * @param length    number of elements in amplitude and phase arrays
     */
    public static WavePattern createConstantPattern(double amplitude, double phase, int length) {
        double[] amp = new double[length];
        double[] phi = new double[length];
        Arrays.fill(amp, amplitude);
        Arrays.fill(phi, phase);
        return new WavePattern(amp, phi);
    }

    /**
     * Creates a WavePattern with linearly increasing amplitude and fixed phase.
     *
     * @param startAmp  starting amplitude
     * @param step      step for amplitude increase
     * @param phase     constant phase
     * @param length    array size
     */
    public static WavePattern createRampAmplitudePattern(double startAmp, double step, double phase, int length) {
        double[] amp = new double[length];
        double[] phi = new double[length];
        for (int i = 0; i < length; i++) {
            amp[i] = startAmp + i * step;
            phi[i] = phase;
        }
        return new WavePattern(amp, phi);
    }

    /**
     * Creates a WavePattern with randomized amplitude and phase, useful for noise simulation.
     */
    public static WavePattern createRandomPattern(int length, long seed) {
        java.util.Random random = new java.util.Random(seed);
        double[] amp = new double[length];
        double[] phi = new double[length];
        for (int i = 0; i < length; i++) {
            amp[i] = random.nextDouble();
            phi[i] = random.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phi);
    }
}