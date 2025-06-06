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
import ai.evacortex.resonancedb.core.math.Complex;

import java.util.List;

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

        Complex[] sa = toComplexWithOptions(a, options);
        Complex[] sb = toComplexWithOptions(b, options);

        if (sa.length != sb.length) {
            throw new IllegalArgumentException("Mismatched lengths: " + sa.length + " vs " + sb.length);
        }

        double energyA = 0.0;
        double energyB = 0.0;
        double interference = 0.0;

        for (int i = 0; i < sa.length; i++) {
            Complex c1 = sa[i];
            Complex c2 = sb[i];

            energyA      += c1.absSquared();
            energyB      += c2.absSquared();
            interference += c1.add(c2).absSquared();
        }

        if (energyA + energyB == 0.0) return 0.0f;

        double base = 0.5 * interference / (energyA + energyB);
        double ampFactor = 2.0 * Math.sqrt(energyA * energyB) / (energyA + energyB);

        return (float) (base * ampFactor);
    }

    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates) {
        return compareMany(query, candidates, CompareOptions.defaultOptions());
    }

    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates, CompareOptions options) {
        if (query == null || candidates == null) {
            throw new NullPointerException("query and candidates must not be null");
        }

        float[] results = new float[candidates.size()];
        for (int i = 0; i < candidates.size(); i++) {
            results[i] = compare(query, candidates.get(i), options);
        }
        return results;
    }

    /**
     * Converts a {@link WavePattern} to an array of {@link Complex} values, applying comparison options.
     * If {@code ignorePhase} is enabled, phases are set to zero (amplitude-only representation).
     *
     * @param pattern the input wave pattern
     * @param options comparison configuration
     * @return array of complex values
     */
    private Complex[] toComplexWithOptions(WavePattern pattern, CompareOptions options) {
        Complex[] complex = pattern.toComplex();

        if (options.ignorePhase()) {
            for (int i = 0; i < complex.length; i++) {
                double mag = complex[i].abs();
                complex[i] = new Complex(mag, 0.0);
            }
        }

        return complex;
    }

    @Override
    public ComparisonResult compareWithPhaseDelta(WavePattern a, WavePattern b) {
        Complex[] wave1 = a.toComplex();
        Complex[] wave2 = b.toComplex();

        if (wave1.length != wave2.length)
            throw new IllegalArgumentException("Pattern length mismatch");

        double phaseDeltaSum = 0.0;
        double energyNumerator = 0.0;
        double energyDenominator = 0.0;

        for (int i = 0; i < wave1.length; i++) {
            Complex c1 = wave1[i];
            Complex c2 = wave2[i];

            Complex sum = c1.add(c2);
            energyNumerator += sum.absSquared();

            double e1 = c1.absSquared();
            double e2 = c2.absSquared();
            energyDenominator += e1 + e2;

            double delta = normalizePhase(c2.phase() - c1.phase());
            phaseDeltaSum += delta;
        }

        float energy = (energyDenominator == 0.0)
                ? 0.0f
                : (float)(0.5 * energyNumerator / energyDenominator);

        double avgPhaseDelta = phaseDeltaSum / wave1.length;

        return new ComparisonResult(energy, avgPhaseDelta);
    }

    private static double normalizePhase(double delta) {
        while (delta <= -Math.PI) delta += 2 * Math.PI;
        while (delta > Math.PI) delta -= 2 * Math.PI;
        return delta;
    }
}