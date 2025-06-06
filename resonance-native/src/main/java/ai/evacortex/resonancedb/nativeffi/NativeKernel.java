/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.nativeffi;

import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;

import java.util.List;

/**
 * {@code NativeKernel} delegates resonance comparison of {@link WavePattern} instances
 * to a native implementation via Panama. This enables hardware-accelerated
 * computation using SIMD or platform-specific optimizations.
 *
 * <p>The similarity score is computed externally and must conform to the standard
 * contract of the {@link ResonanceKernel}, producing values in the range [0.0 ... 1.0].</p>
 *
 * <p>Internally, this class transforms double-precision {@code amplitude} and {@code phase}
 * arrays into float arrays suitable for native comparison.</p>
 *
 * <p>Use cases include high-performance execution paths or CPU-specific tuning
 * of wave-based semantic matching.</p>
 *
 * @author Alexander Listopad
 * @see ResonanceKernel
 * @see NativeCompare
 */
/**
 * {@code NativeKernel} provides a SIMD-accelerated implementation of {@link ResonanceKernel}
 * using a native backend via the Panama Vector API or JNI-based bridge.
 *
 * <p>It computes phase-sensitive resonance similarity scores between wave patterns represented as:</p>
 * <pre>
 *     ψ(x) = A(x) · e^{iφ(x)}
 * </pre>
 *
 * <p>All computations are performed on {@code float[]} arrays, with {@code double[]} inputs cast down.
 * If {@link CompareOptions#ignorePhase()} is enabled, this kernel falls back to {@link JavaKernel},
 * as native phase-agnostic mode is not yet supported.</p>
 *
 * <p>Like all {@code ResonanceKernel} implementations, results are deterministic, symmetric,
 * and side-effect free.</p>
 *
 * @see ResonanceKernel
 * @see JavaKernel
 * @see NativeCompare
 */
public final class NativeKernel implements ResonanceKernel {

    /**
     * Compares two wave patterns using default comparison options.
     *
     * <p>Delegates to {@link #compare(WavePattern, WavePattern, CompareOptions)} with
     * {@code CompareOptions.defaultOptions()}.</p>
     */
    @Override
    public float compare(WavePattern a, WavePattern b) {
        return compare(a, b, CompareOptions.defaultOptions());
    }

    /**
     * Computes the resonance similarity score using native SIMD backend.
     *
     * <p>If {@code ignorePhase} is enabled in options, this method delegates to {@link JavaKernel},
     * as the native implementation does not yet support phase-insensitive mode.</p>
     *
     * @param a       the first wave pattern
     * @param b       the second wave pattern
     * @param options comparison options
     * @return similarity score in [0.0 ... 1.0]
     * @throws NullPointerException     if any input is {@code null}
     * @throws RuntimeException         if native call fails
     */
    @Override
    public float compare(WavePattern a, WavePattern b, CompareOptions options) {
        if (a == null || b == null) {
            throw new NullPointerException("WavePatterns must not be null");
        }

        if (options.ignorePhase()) {
            // Fallback to JavaKernel for non-phase comparisons
            return new JavaKernel().compare(a, b, options);
        }

        try {
            float[] amp1 = toFloatArray(a.amplitude());
            float[] phase1 = toFloatArray(a.phase());
            float[] amp2 = toFloatArray(b.amplitude());
            float[] phase2 = toFloatArray(b.phase());

            return NativeCompare.compare(amp1, phase1, amp2, phase2);
        } catch (Throwable e) {
            throw new RuntimeException("Native comparison failed", e);
        }
    }

    /**
     * Compares a query pattern to a list of candidates using default options.
     *
     * @param query      the reference pattern
     * @param candidates the list of candidates
     * @return array of similarity scores
     */
    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates) {
        return compareMany(query, candidates, CompareOptions.defaultOptions());
    }

    /**
     * Performs SIMD-accelerated batch comparison between a query and multiple candidates.
     *
     * <p>Only phase-sensitive comparisons are supported natively. If {@code ignorePhase}
     * is enabled in options, this method throws {@link UnsupportedOperationException}.</p>
     *
     * @param query      the reference pattern
     * @param candidates list of patterns to compare with
     * @param options    comparison configuration
     * @return array of similarity scores
     * @throws NullPointerException              if any argument is {@code null}
     * @throws UnsupportedOperationException     if {@code ignorePhase} is {@code true}
     * @throws RuntimeException                  if native call fails
     */
    @Override
    public float[] compareMany(WavePattern query, List<WavePattern> candidates, CompareOptions options) {
        if (query == null || candidates == null) {
            throw new NullPointerException("Query and candidates must not be null");
        }

        if (options.ignorePhase()) {
            throw new UnsupportedOperationException("NativeKernel does not support ignorePhase yet");
        }

        try {
            float[] ampQ = toFloatArray(query.amplitude());
            float[] phaseQ = toFloatArray(query.phase());

            float[][] ampList = new float[candidates.size()][];
            float[][] phaseList = new float[candidates.size()][];

            for (int i = 0; i < candidates.size(); i++) {
                ampList[i] = toFloatArray(candidates.get(i).amplitude());
                phaseList[i] = toFloatArray(candidates.get(i).phase());
            }

            return NativeCompare.compareMany(ampQ, phaseQ, ampList, phaseList);
        } catch (Throwable e) {
            throw new RuntimeException("Native batch comparison failed", e);
        }
    }

    /**
     * Converts a {@code double[]} array to a {@code float[]} array by casting.
     *
     * @param input array of doubles
     * @return array of floats
     */
    private float[] toFloatArray(double[] input) {
        float[] out = new float[input.length];
        for (int i = 0; i < input.length; i++) {
            out[i] = (float) input[i];
        }
        return out;
    }

    /**
     * Computes the raw resonance energy and average signed phase difference (Δφ) between two wave patterns,
     * using a native SIMD-accelerated backend.
     *
     * <p>This method delegates to {@link NativeCompare#compareWithPhaseDelta(float[], float[], float[], float[])}
     * and computes both:</p>
     * <ul>
     *     <li>{@link ComparisonResult#energy()} — normalized resonance energy without amplitude compensation</li>
     *     <li>{@link ComparisonResult#phaseDelta()} — average signed phase shift in radians ∈ [–π, +π]</li>
     * </ul>
     *
     * <p>The result reflects raw constructive interference and phase alignment without normalization,
     * and is primarily intended for zone classification, diagnostics, or threshold analysis.</p>
     *
     * <p>Behavior conforms to the {@link ResonanceKernel} contract and guarantees:</p>
     * <ul>
     *     <li>Determinism: same input yields same result</li>
     *     <li>Symmetry: compare(a, b) == compare(b, a)</li>
     *     <li>No side effects</li>
     * </ul>
     *
     * @param a the first wave pattern (ψ₁)
     * @param b the second wave pattern (ψ₂)
     * @return {@code ComparisonResult} containing raw resonance energy and signed phase delta
     * @throws NullPointerException if either pattern is {@code null}
     * @throws IllegalArgumentException if pattern lengths differ
     * @throws RuntimeException if the native call fails
     */
    @Override
    public ComparisonResult compareWithPhaseDelta(WavePattern a, WavePattern b) {
        if (a == null || b == null) {
            throw new NullPointerException("Patterns must not be null");
        }
        if (a.amplitude().length != b.amplitude().length) {
            throw new IllegalArgumentException("Pattern length mismatch");
        }

        float[] amp1 = toFloatArray(a.amplitude());
        float[] phase1 = toFloatArray(a.phase());
        float[] amp2 = toFloatArray(b.amplitude());
        float[] phase2 = toFloatArray(b.phase());

        try {
            float[] out = NativeCompare.compareWithPhaseDelta(amp1, phase1, amp2, phase2);
            return new ComparisonResult(out[0], out[1]);
        } catch (Throwable e) {
            throw new RuntimeException("Native compareWithPhaseDelta failed", e);
        }
    }
}