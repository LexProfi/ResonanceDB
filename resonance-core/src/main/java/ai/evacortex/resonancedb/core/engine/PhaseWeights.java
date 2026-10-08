/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

import java.util.Arrays;

/**
 * Immutable, thread-safe container for per-dimension phase participation weights.
 *
 * <p>Each weight controls how much the phase of the corresponding dimension
 * contributes to the resonance score:</p>
 * <ul>
 *     <li>{@code 1.0} — full phase sensitivity (default behavior)</li>
 *     <li>{@code 0.0} — phase ignored, amplitude still participates</li>
 *     <li>{@code 0 < w < 1} — partial phase participation (smooth interpolation)</li>
 * </ul>
 *
 * <p>This is a domain-neutral numerical construct. ResonanceDB does not assign
 * semantic meaning to individual dimensions.</p>
 */
public final class PhaseWeights {

    private final double[] weights;
    private final double sumOfWeights;
    private final boolean isDefault;
    private final boolean isPhaseFree;

    /**
     * Creates phase weights from a defensive copy of the given array.
     *
     * @param weights per-dimension weights, each in [0.0, 1.0]
     * @throws IllegalArgumentException if any weight is out of range, NaN, or Infinity
     * @throws NullPointerException if weights is null
     */
    public PhaseWeights(double[] weights) {
        if (weights == null) {
            throw new NullPointerException("weights must not be null");
        }
        if (weights.length == 0) {
            throw new IllegalArgumentException("weights must not be empty");
        }

        this.weights = new double[weights.length];
        double sum = 0.0;
        boolean allOne = true;
        boolean allZero = true;

        for (int i = 0; i < weights.length; i++) {
            double w = weights[i];
            if (Double.isNaN(w)) {
                throw new IllegalArgumentException("weight[" + i + "] is NaN");
            }
            if (Double.isInfinite(w)) {
                throw new IllegalArgumentException("weight[" + i + "] is Infinite");
            }
            if (w < 0.0) {
                throw new IllegalArgumentException("weight[" + i + "] = " + w + " is negative");
            }
            if (w > 1.0) {
                throw new IllegalArgumentException("weight[" + i + "] = " + w + " exceeds 1.0");
            }
            this.weights[i] = w;
            sum += w;
            if (w != 1.0) allOne = false;
            if (w != 0.0) allZero = false;
        }

        this.sumOfWeights = sum;
        this.isDefault = allOne;
        this.isPhaseFree = allZero;
    }

    /**
     * Returns the weight for the given dimension index.
     */
    public double weight(int i) {
        return weights[i];
    }

    /**
     * Returns a defensive copy of the underlying weights array.
     */
    public double[] toArray() {
        return weights.clone();
    }

    /**
     * Returns the internal weights array without copying.
     * Callers must not modify the returned array.
     */
    public double[] rawWeights() {
        return weights;
    }

    /**
     * Returns the number of dimensions.
     */
    public int length() {
        return weights.length;
    }

    /**
     * Returns the sum of all weights.
     */
    public double sumOfWeights() {
        return sumOfWeights;
    }

    /**
     * Returns {@code true} if all weights are 1.0 (equivalent to default behavior).
     */
    public boolean isDefault() {
        return isDefault;
    }

    /**
     * Returns {@code true} if all weights are 0.0 (phase-free / amplitude-only).
     */
    public boolean isPhaseFree() {
        return isPhaseFree;
    }

    /**
     * Validates that this PhaseWeights has the expected dimension count.
     *
     * @param expectedDimension the expected number of dimensions
     * @throws IllegalArgumentException if dimensions don't match
     */
    public void validateDimension(int expectedDimension) {
        if (weights.length != expectedDimension) {
            throw new IllegalArgumentException(
                    "PhaseWeights dimension mismatch: expected " + expectedDimension
                            + ", got " + weights.length);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PhaseWeights other)) return false;
        return Arrays.equals(weights, other.weights);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(weights);
    }

    @Override
    public String toString() {
        if (isDefault) return "PhaseWeights[all-one, dim=" + weights.length + "]";
        if (isPhaseFree) return "PhaseWeights[all-zero, dim=" + weights.length + "]";
        return "PhaseWeights[dim=" + weights.length + ", sumW=" + String.format("%.3f", sumOfWeights) + "]";
    }
}
