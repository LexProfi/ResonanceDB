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

/**
 * ResonanceKernel defines the interface for computing similarity between two {@link WavePattern}
 * instances using principles of constructive interference between complex waveforms.
 *
 * <p>Each wave pattern is treated as a complex-valued function of the form:</p>
 * <pre>
 *     ψ(x) = A(x) · e^{iφ(x)}
 * </pre>
 *
 * <p>The similarity score is computed based on the combined resonance energy of the waveforms,
 * normalized to the range [0.0 ... 1.0]. A score of:</p>
 * <ul>
 *     <li>1.0 indicates perfect constructive interference (identical amplitude and phase)</li>
 *     <li>0.0 indicates complete destructive interference (opposite phase)</li>
 * </ul>
 *
 * <p>Implementations may support advanced comparison strategies through {@link CompareOptions},
 * including amplitude normalization, phase sensitivity, or global phase shift compensation.</p>
 *
 * @see WavePattern
 * @see CompareOptions
 * @see JavaKernel
 */
public interface ResonanceKernel {

    /**
     * Computes the normalized resonance similarity between two wave patterns.
     *
     * <p>This method uses default comparison options (phase-sensitive, no pre-normalization).</p>
     *
     * @param a the first wave pattern (ψ₁)
     * @param b the second wave pattern (ψ₂)
     * @return similarity score in the range [0.0 ... 1.0]
     * @throws IllegalArgumentException if patterns are of unequal length
     * @throws NullPointerException if either input is {@code null}
     */
    float compare(WavePattern a, WavePattern b);

    /**
     * Computes the resonance similarity between two wave patterns using configurable options.
     *
     * <p>This method allows customization of comparison behavior via {@link CompareOptions}, such as:</p>
     * <ul>
     *     <li>Ignoring phase (magnitude-only comparison)</li>
     *     <li>Pre-normalizing amplitudes</li>
     *     <li>Future extensions like global phase shift alignment</li>
     * </ul>
     *
     * <p>The core computation evaluates the energy of the combined waveform relative to
     * the individual energies of each pattern, with amplitude balancing to compensate for
     * energy asymmetry:</p>
     * <pre>
     *     E = 0.5 · |ψ₁ + ψ₂|² / (|ψ₁|² + |ψ₂|²) · A
     *     A = 2 · √(E₁ · E₂) / (E₁ + E₂)
     * </pre>
     *
     * @param a the first wave pattern (ψ₁)
     * @param b the second wave pattern (ψ₂)
     * @param options comparison configuration
     * @return similarity score in the range [0.0 ... 1.0]
     * @throws IllegalArgumentException if patterns are of unequal length or options are unsupported
     * @throws NullPointerException if any input is {@code null}
     */
    float compare(WavePattern a, WavePattern b, CompareOptions options);
}