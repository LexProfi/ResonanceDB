/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

import ai.evacortex.resonancedb.core.WavePattern;

import java.util.List;

/**
 * {@code ResonanceKernel} defines the interface for computing similarity between two {@link WavePattern}
 * instances using the principle of constructive interference between complex-valued waveforms.
 *
 * <p>Each wave pattern ψ(x) is treated as a function of the form:</p>
 * <pre>
 *     ψ(x) = A(x) · e^{iφ(x)}
 * </pre>
 *
 * <p>Similarity is measured as the normalized resonance energy of the superposition ψ₁ + ψ₂,
 * relative to the total energy of both inputs, optionally corrected by an amplitude compensation factor.
 * The result is always bounded within [0.0 ... 1.0].</p>
 *
 * <p>A score of:</p>
 * <ul>
 *     <li>1.0 indicates perfect constructive interference (aligned phase and balanced amplitude)</li>
 *     <li>0.0 indicates complete destructive interference (opposite phase or null amplitude)</li>
 * </ul>
 *
 * <p>The core comparison formula is:</p>
 * <pre>
 *     E = 0.5 · |ψ₁ + ψ₂|² / (|ψ₁|² + |ψ₂|²) · A
 *     A = 2 · √(E₁ · E₂) / (E₁ + E₂)
 * </pre>
 *
 * <p>Implementations must be deterministic, symmetric, and free of side effects.
 * They may support advanced options via {@link CompareOptions}, including:</p>
 * <ul>
 *     <li>{@code ignorePhase} — disable phase sensitivity</li>
 *     <li>{@code normalizeAmplitude} — adjust for amplitude scale</li>
 *     <li>{@code allowGlobalPhaseShift} — align global φ offset (future)</li>
 * </ul>
 *
 * @see WavePattern
 * @see CompareOptions
 * @see JavaKernel
 */
public interface ResonanceKernel {

    /**
     * Computes the resonance similarity score between two wave patterns using default comparison options.
     *
     * <p>This method is equivalent to {@code compare(a, b, CompareOptions.defaultOptions())} and assumes:</p>
     * <ul>
     *     <li>Phase sensitivity is enabled</li>
     *     <li>No amplitude normalization</li>
     *     <li>No phase alignment</li>
     * </ul>
     *
     * @param a the first wave pattern (ψ₁)
     * @param b the second wave pattern (ψ₂)
     * @return similarity score in [0.0 ... 1.0]
     * @throws IllegalArgumentException if pattern lengths differ
     * @throws NullPointerException if either input is {@code null}
     */
    float compare(WavePattern a, WavePattern b);

    /**
     * Computes the resonance similarity score between two wave patterns using configurable options.
     *
     * <p>The formula remains mathematically consistent with the normalized interference energy model:</p>
     * <pre>
     *     E = 0.5 · |ψ₁ + ψ₂|² / (|ψ₁|² + |ψ₂|²) · A
     *     A = 2 · √(E₁ · E₂) / (E₁ + E₂)
     * </pre>
     *
     * @param a the first wave pattern (ψ₁)
     * @param b the second wave pattern (ψ₂)
     * @param options comparison configuration flags
     * @return similarity score in [0.0 ... 1.0]
     * @throws IllegalArgumentException if pattern lengths mismatch or unsupported options are used
     * @throws NullPointerException if any argument is {@code null}
     */
    float compare(WavePattern a, WavePattern b, CompareOptions options);

    /**
     * Computes similarity scores between a query wave pattern and a list of candidate patterns.
     *
     * <p>This is a batch variant of {@link #compare(WavePattern, WavePattern)}.</p>
     *
     * @param query the reference/query pattern
     * @param candidates list of candidates to compare with
     * @return array of similarity scores, one per candidate
     * @throws IllegalArgumentException if lengths mismatch
     * @throws NullPointerException if any argument is {@code null}
     */
    float[] compareMany(WavePattern query, List<WavePattern> candidates);

    /**
     * Computes similarity scores between a query pattern and multiple candidates using specified options.
     *
     * <p>Each candidate is compared independently using {@link #compare(WavePattern, WavePattern, CompareOptions)}.</p>
     *
     * @param query the reference/query pattern
     * @param candidates list of candidates to compare with
     * @param options comparison configuration flags
     * @return array of similarity scores in [0.0 ... 1.0]
     * @throws IllegalArgumentException if any pattern is invalid
     * @throws NullPointerException if any input is {@code null}
     */
    float[] compareMany(WavePattern query, List<WavePattern> candidates, CompareOptions options);
}