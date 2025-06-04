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

/**
 * {@code JavaKernel} provides a resonance-based similarity metric for comparing two {@link WavePattern}
 * instances, based on constructive interference of complex-valued waveforms.
 *
 * <p>The kernel computes a normalized resonance energy that accounts for both phase alignment
 * and amplitude balance. The formula ensures bounded output in the range [0.0 ... 1.0] without
 * requiring artificial clipping.</p>
 *
 * <p>Resonance energy is defined as:</p>
 *
 * <pre>{@code
 *     E = 0.5 * |ψ₁ + ψ₂|² / (|ψ₁|² + |ψ₂|²) * A
 *     A = 2 * sqrt(E₁ * E₂) / (E₁ + E₂)
 * }</pre>
 *
 * <p>Where ψ(x) = A(x) · e^{iφ(x)} is the complex representation of the wave pattern.</p>
 *
 * <ul>
 *     <li>E = 1.0 for complete constructive interference (Δφ = 0, equal amplitudes)</li>
 *     <li>E = 0.0 for perfect destructive interference (Δφ = π)</li>
 *     <li>The amplitude factor A compensates for energy imbalance between patterns</li>
 * </ul>
 *
 * <p>The comparison is symmetric, differentiable, and suitable for reasoning systems
 * based on wave-like cognitive structures.</p>
 *
 * <p>{@link CompareOptions} can be used to enable or disable phase sensitivity and other
 * normalization strategies.</p>
 *
 * @author Alexander Listopad
 * @see ResonanceKernel
 * @see WavePattern
 */
public final class JavaKernel implements ResonanceKernel {

    @Override
    public float compare(WavePattern a, WavePattern b) {
        return compare(a, b, CompareOptions.defaultOptions());
    }

    @Override
    public float compare(WavePattern a,
                         WavePattern b,
                         CompareOptions options) {

        if (a == null || b == null) {
            throw new NullPointerException("patterns must not be null");
        }

        Complex[] sa = a.toComplex();
        Complex[] sb = b.toComplex();
        if (sa.length != sb.length) {
            throw new IllegalArgumentException("Mismatched lengths: " + sa.length + " vs " + sb.length);
        }

        double energyA = 0.0;
        double energyB = 0.0;
        double interference = 0.0;

        for (int i = 0; i < sa.length; i++) {
            Complex c1 = sa[i];
            Complex c2 = sb[i];

            if (options.ignorePhase()) {
                c1 = new Complex(c1.abs(), 0.0);
                c2 = new Complex(c2.abs(), 0.0);
            }

            energyA      += c1.absSquared();
            energyB      += c2.absSquared();
            interference += c1.add(c2).absSquared();
        }

        if (energyA + energyB == 0.0) return 0.0f;

        double base = 0.5 * interference / (energyA + energyB);
        double ampFactor = 2.0 * Math.sqrt(energyA * energyB) / (energyA + energyB);

        return (float) (base * ampFactor);
    }
}