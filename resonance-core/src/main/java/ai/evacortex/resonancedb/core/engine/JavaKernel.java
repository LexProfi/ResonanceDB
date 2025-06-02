/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.engine;

import ai.evacortex.resonancedb.core.WavePattern;
import ai.evacortex.resonancedb.core.math.Complex;

public class JavaKernel implements ResonanceKernel {

    @Override
    public float compare(WavePattern a, WavePattern b) {

        if (a == null || b == null) throw new IllegalArgumentException("WavePatterns must not be null");

        Complex[] sa = a.toComplex();
        Complex[] sb = b.toComplex();

        if (sa.length != sb.length) {
            throw new IllegalArgumentException("WavePatterns must have the same length");
        }

        double numerator = 0.0;
        double energyA = 0.0;
        double energyB = 0.0;

        for (int i = 0; i < sa.length; i++) {
            Complex sum = sa[i].add(sb[i]);
            numerator += sum.absSquared();

            energyA += sa[i].absSquared();
            energyB += sb[i].absSquared();
        }

        double denom = energyA + energyB;
        return denom == 0 ? 0.0f : (float) (numerator / denom);
    }
}
