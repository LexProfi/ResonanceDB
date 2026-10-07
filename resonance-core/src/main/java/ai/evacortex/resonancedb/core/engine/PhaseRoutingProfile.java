/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

import ai.evacortex.resonancedb.core.storage.WavePattern;

/**
 * Domain-neutral internal routing context derived from {@link CompareOptions}.
 *
 * <p>This profile encapsulates numerical routing properties needed by phase-shard selection
 * and IVF centroid ranking when using parametric phase weights. It carries no semantic meaning —
 * only numerical geometry information.</p>
 *
 * <p>Instances are immutable and thread-safe.</p>
 */
public final class PhaseRoutingProfile {

    private static final PhaseRoutingProfile DEFAULT =
            new PhaseRoutingProfile(null, CompareOptions.defaultOptions());

    private final PhaseWeights phaseWeights;
    private final boolean isDefault;
    private final boolean isPhaseFree;
    private final double sumOfWeights;
    private final double meanParticipation;
    private final double weightDrift;
    private final double routingUncertainty;

    /**
     * Creates a routing profile from the given compare options.
     *
     * @param phaseWeights the phase weights (may be null for default behavior)
     * @param options the compare options
     */
    public PhaseRoutingProfile(PhaseWeights phaseWeights, CompareOptions options) {
        if (options.isEffectivelyPhaseFree()) {
            this.phaseWeights = phaseWeights;
            this.isDefault = false;
            this.isPhaseFree = true;
            this.sumOfWeights = 0.0;
            this.meanParticipation = 0.0;
            this.weightDrift = 1.0;
            this.routingUncertainty = Math.PI; // full circle — no phase routing possible
        } else if (phaseWeights == null || phaseWeights.isDefault()) {
            this.phaseWeights = phaseWeights;
            this.isDefault = true;
            this.isPhaseFree = false;
            this.sumOfWeights = phaseWeights != null ? phaseWeights.sumOfWeights() : 0.0;
            this.meanParticipation = 1.0;
            this.weightDrift = 0.0;
            this.routingUncertainty = 0.0;
        } else {
            this.phaseWeights = phaseWeights;
            this.isDefault = false;
            this.isPhaseFree = false;
            int N = phaseWeights.length();
            double sumW = phaseWeights.sumOfWeights();
            this.sumOfWeights = sumW;
            this.meanParticipation = sumW / N;

            // Weight drift: 0.5 * Σ |α_i - u_i| where α_i = w_i/Σw, u_i = 1/N
            // Measures how much the normalized weight distribution deviates from uniform
            double uniformWeight = 1.0 / N;
            double driftSum = 0.0;
            double[] w = phaseWeights.rawWeights();
            for (int i = 0; i < N; i++) {
                driftSum += Math.abs(w[i] / sumW - uniformWeight);
            }
            this.weightDrift = 0.5 * driftSum;

            // Routing uncertainty: tight conservative bound on maximum possible difference
            // between a pattern's full phase center and its weighted phase center.
            //
            // For a pattern with phases φ_1,...,φ_N ∈ (-π, π]:
            //   fullMean = (1/N) Σ φ_i
            //   weightedMean = (Σ w_i φ_i) / (Σ w_i)
            //   |weightedMean - fullMean| = |Σ (α_i - u_i) φ_i|
            //
            // With coefficients summing to 0 and φ_i ∈ [-π, π], the tight bound is:
            //   δ = π × Σ |α_i - u_i| = π × 2 × weightDrift
            this.routingUncertainty = Math.PI * driftSum;
        }
    }

    /**
     * Returns the default routing profile (all-one weights, zero uncertainty).
     */
    public static PhaseRoutingProfile defaultProfile() {
        return DEFAULT;
    }

    /**
     * Creates a routing profile from compare options.
     */
    public static PhaseRoutingProfile from(CompareOptions options) {
        if (options == null) return DEFAULT;
        return new PhaseRoutingProfile(options.phaseWeights(), options);
    }

    public PhaseWeights phaseWeights() { return phaseWeights; }
    public boolean isDefault() { return isDefault; }
    public boolean isPhaseFree() { return isPhaseFree; }
    public double sumOfWeights() { return sumOfWeights; }
    public double meanParticipation() { return meanParticipation; }
    public double weightDrift() { return weightDrift; }
    public double routingUncertainty() { return routingUncertainty; }

    /**
     * Computes the weighted phase center of a query pattern using this profile's weights.
     *
     * <p>For default weights, returns the standard arithmetic mean.
     * For phase-free, returns 0.0 (no meaningful center).</p>
     *
     * @param query the query pattern
     * @return weighted phase center in radians
     */
    public double weightedPhaseCenter(WavePattern query) {
        if (isPhaseFree) return 0.0;

        double[] phase = query.phase();
        int N = phase.length;

        if (isDefault || phaseWeights == null) {
            double sum = 0.0;
            for (double p : phase) sum += p;
            return sum / N;
        }

        double[] w = phaseWeights.rawWeights();
        if (w.length != N) {
            throw new IllegalArgumentException(
                    "PhaseWeights dimension mismatch: expected " + N + ", got " + w.length);
        }
        double wSum = 0.0, phaseSum = 0.0;
        for (int i = 0; i < N; i++) {
            wSum += w[i];
            phaseSum += w[i] * phase[i];
        }
        return wSum > 0.0 ? phaseSum / wSum : 0.0;
    }

    @Override
    public String toString() {
        if (isDefault) return "PhaseRoutingProfile[default]";
        if (isPhaseFree) return "PhaseRoutingProfile[phase-free]";
        return "PhaseRoutingProfile[meanPart=" + String.format("%.3f", meanParticipation)
                + ", drift=" + String.format("%.3f", weightDrift)
                + ", uncertainty=" + String.format("%.4f", routingUncertainty) + "]";
    }
}
