/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.PhaseWeights;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for amplitude-weighted phaseDelta correctness (Issue A, defects D1–D4).
 *
 * <p>Verifies that {@code compareWithPhaseDelta()} returns the amplitude-weighted
 * resultant angle of the complex cross-correlation:
 * {@code θ = atan2(Σ A1·A2·sin(Δφ), Σ A1·A2·cos(Δφ))}</p>
 */
@DisplayName("PhaseDelta amplitude-weighted correctness")
class PhaseDeltaCorrectnessTest {

    private static final ResonanceKernel KERNEL = new JavaKernel();

    private static double referencePhaseDelta(double[] a1, double[] p1,
                                              double[] a2, double[] p2) {
        double sinSum = 0.0, cosSum = 0.0;
        for (int i = 0; i < a1.length; i++) {
            double ap = a1[i] * a2[i];
            double dphi = p2[i] - p1[i];
            sinSum += ap * Math.sin(dphi);
            cosSum += ap * Math.cos(dphi);
        }
        return (sinSum == 0.0 && cosSum == 0.0) ? 0.0 : Math.atan2(sinSum, cosSum);
    }

    private static double referencePhaseDeltaWeighted(double[] a1, double[] p1,
                                                      double[] a2, double[] p2,
                                                      double[] w) {
        double sinSum = 0.0, cosSum = 0.0;
        for (int i = 0; i < a1.length; i++) {
            double wap = w[i] * a1[i] * a2[i];
            double dphi = p2[i] - p1[i];
            sinSum += wap * Math.sin(dphi);
            cosSum += wap * Math.cos(dphi);
        }
        return (sinSum == 0.0 && cosSum == 0.0) ? 0.0 : Math.atan2(sinSum, cosSum);
    }

    // ── Task diagnostic case ──

    @Test
    @DisplayName("Strongly unequal amplitudes: phaseDelta dominated by high-amplitude dim")
    void stronglyUnequalAmplitudes_phaseDeltaDominatedByHighAmplitude() {
        WavePattern a = new WavePattern(new double[]{1.0, 0.1}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 0.1}, new double[]{0.0, Math.PI / 2});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);

        // The amplitude-weighted resultant: atan2(0.01*sin(π/2), 1.0*cos(0)+0.01*cos(π/2))
        // = atan2(0.01, 1.0) ≈ 0.009999667 rad
        double expected = Math.atan2(0.01, 1.0);
        assertEquals(expected, r.phaseDelta(), 1e-9,
                "phaseDelta should be ~0.01 rad, not π/4 — high-amp dim dominates");
    }

    // ── Equal amplitudes ──

    @Test
    @DisplayName("Equal amplitudes: phaseDelta matches simple circular mean")
    void equalAmplitudes_matchesCircularMean() {
        double amp = 1.0;
        WavePattern a = new WavePattern(new double[]{amp, amp}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{amp, amp}, new double[]{0.0, Math.PI / 2});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        // With equal amplitudes, amp-weighted ≡ unweighted circular mean:
        // atan2(sin(0)+sin(π/2), cos(0)+cos(π/2)) = atan2(1, 1) = π/4
        assertEquals(Math.PI / 4, r.phaseDelta(), 1e-9);
    }

    // ── Zero-amplitude coordinates ──

    @Test
    @DisplayName("Zero-amplitude coordinate does not influence phaseDelta")
    void zeroAmplitudeCoord_noInfluence() {
        // Dim 0: both have amp=1.0, dphi=0.2
        // Dim 1: both have amp=0.0, dphi=π (would pull strongly if unweighted)
        WavePattern a = new WavePattern(new double[]{1.0, 0.0}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 0.0}, new double[]{0.2, Math.PI});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        assertEquals(0.2, r.phaseDelta(), 1e-9,
                "Zero-amp dim must not affect phaseDelta");
    }

    // ── Entirely zero-energy patterns ──

    @Test
    @DisplayName("Both patterns zero energy → phaseDelta = 0.0")
    void bothZeroEnergy_phaseDeltaIsZero() {
        WavePattern a = new WavePattern(new double[]{0.0, 0.0}, new double[]{0.5, 1.0});
        WavePattern b = new WavePattern(new double[]{0.0, 0.0}, new double[]{1.5, 2.0});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        assertEquals(0.0f, r.energy());
        assertEquals(0.0, r.phaseDelta());
    }

    @Test
    @DisplayName("One pattern zero energy → phaseDelta = 0.0")
    void oneZeroEnergy_phaseDeltaIsZero() {
        WavePattern a = new WavePattern(new double[]{1.0, 1.0}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{0.0, 0.0}, new double[]{0.5, 1.0});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        assertEquals(0.0f, r.energy());
        assertEquals(0.0, r.phaseDelta());
    }

    // ── Opposite phases ──

    @Test
    @DisplayName("Opposite phases → |phaseDelta| ≈ π")
    void oppositePhases_phaseDeltaNearPi() {
        WavePattern a = new WavePattern(new double[]{1.0, 1.0, 1.0}, new double[]{0.0, 0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 1.0, 1.0},
                new double[]{Math.PI, Math.PI, Math.PI});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        assertTrue(Math.abs(Math.abs(r.phaseDelta()) - Math.PI) < 1e-6);
    }

    // ── Orthogonal phases ──

    @Test
    @DisplayName("Orthogonal phases (π/2 shift) → phaseDelta ≈ π/2")
    void orthogonalPhases_phaseDeltaPiOver2() {
        WavePattern a = new WavePattern(new double[]{1.0, 1.0}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 1.0},
                new double[]{Math.PI / 2, Math.PI / 2});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        assertEquals(Math.PI / 2, r.phaseDelta(), 1e-9);
    }

    // ── Phase wrapping ──

    @Test
    @DisplayName("Phase wrapping: large phase values wrap correctly")
    void phaseWrapping_largePhasesHandledCorrectly() {
        double ph1 = 100.0; // many wraps of 2π
        double ph2 = ph1 + 0.3;
        WavePattern a = new WavePattern(new double[]{1.0}, new double[]{ph1});
        WavePattern b = new WavePattern(new double[]{1.0}, new double[]{ph2});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        assertEquals(0.3, r.phaseDelta(), 1e-6, "Phase wrapping should produce correct delta");
    }

    // ── Near-zero complex resultant (cancellation) ──

    @Test
    @DisplayName("Symmetric cancellation: resultant near zero → phaseDelta = 0.0")
    void symmetricCancellation_phaseDeltaIsZero() {
        // Two dims with equal amplitude, opposite dphi → sin terms cancel
        WavePattern a = new WavePattern(new double[]{1.0, 1.0}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 1.0},
                new double[]{0.5, -0.5});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        // sinSum = sin(0.5) + sin(-0.5) = 0, cosSum = cos(0.5) + cos(-0.5) = 2*cos(0.5)
        assertEquals(0.0, r.phaseDelta(), 1e-9,
                "Symmetric sin cancellation should yield phaseDelta = 0");
    }

    // ── Complete destructive cancellation ──

    @Test
    @DisplayName("Complete destructive cancellation with equal amplitudes")
    void completeDestructiveCancellation() {
        // dphi[0] = π/3, dphi[1] = -π/3, equal amps → sinSum cancels, cosSum = 2*cos(π/3)
        WavePattern a = new WavePattern(new double[]{1.0, 1.0}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 1.0},
                new double[]{Math.PI / 3, -Math.PI / 3});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        assertEquals(0.0, r.phaseDelta(), 1e-9);
    }

    // ── ignorePhase=true ──

    @Test
    @DisplayName("ignorePhase=true → phaseDelta = 0.0 and energy is phase-free")
    void ignorePhase_phaseDeltaIsZero() {
        WavePattern a = new WavePattern(new double[]{1.0, 0.5}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 0.5}, new double[]{0.3, 1.7});

        CompareOptions opts = new CompareOptions(false, true, false, false);
        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b, opts);

        assertEquals(0.0, r.phaseDelta(), 0.0, "ignorePhase must yield phaseDelta=0.0");
        float pfScore = KERNEL.compare(a, b, opts);
        assertEquals(pfScore, r.energy(), 1e-6, "energy must match phase-free compare()");
    }

    // ── Weighted phase comparisons ──

    @Test
    @DisplayName("PhaseWeights: masked dimension does not affect phaseDelta")
    void phaseWeights_maskedDimIgnored() {
        // Dim 0: w=1.0, dphi=0.2, amps=1.0
        // Dim 1: w=0.0, dphi=π, amps=1.0 (would dominate if not masked)
        WavePattern a = new WavePattern(new double[]{1.0, 1.0}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 1.0}, new double[]{0.2, Math.PI});

        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(new double[]{1.0, 0.0}));
        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b, opts);
        assertEquals(0.2, r.phaseDelta(), 1e-9,
                "Masked dim (w=0) must not influence phaseDelta");
    }

    @Test
    @DisplayName("PhaseWeights with varying amplitude: amplitude weighting applies")
    void phaseWeights_amplitudeWeightingApplies() {
        WavePattern a = new WavePattern(new double[]{1.0, 0.1}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{1.0, 0.1}, new double[]{0.0, Math.PI / 2});

        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(new double[]{1.0, 1.0}));
        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b, opts);

        double expected = referencePhaseDeltaWeighted(
                a.amplitude(), a.phase(), b.amplitude(), b.phase(),
                new double[]{1.0, 1.0});
        assertEquals(expected, r.phaseDelta(), 1e-9);
    }

    // ── Symmetry and argument reversal ──

    @Test
    @DisplayName("phaseDelta(a, b) = -phaseDelta(b, a) (antisymmetry)")
    void antisymmetry() {
        WavePattern a = new WavePattern(new double[]{1.0, 0.5, 0.8}, new double[]{0.1, 0.5, 1.2});
        WavePattern b = new WavePattern(new double[]{0.7, 0.9, 0.3}, new double[]{0.4, 1.1, 0.2});

        ComparisonResult ab = KERNEL.compareWithPhaseDelta(a, b);
        ComparisonResult ba = KERNEL.compareWithPhaseDelta(b, a);

        assertEquals(ab.energy(), ba.energy(), 1e-6, "Energy must be symmetric");
        assertEquals(ab.phaseDelta(), -ba.phaseDelta(), 1e-9,
                "phaseDelta must be antisymmetric");
    }

    // ── Fuzz: matches reference formula ──

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7, 17, 127, 513})
    @DisplayName("Fuzz: phaseDelta matches amplitude-weighted reference")
    void fuzz_matchesReference(int dim) {
        Random rng = new Random(dim * 31L);
        for (int t = 0; t < 50; t++) {
            double[] a1 = new double[dim], p1 = new double[dim];
            double[] a2 = new double[dim], p2 = new double[dim];
            for (int i = 0; i < dim; i++) {
                a1[i] = rng.nextDouble() * 2;
                p1[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
                a2[i] = rng.nextDouble() * 2;
                p2[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
            }
            WavePattern pa = new WavePattern(a1, p1);
            WavePattern pb = new WavePattern(a2, p2);

            ComparisonResult r = KERNEL.compareWithPhaseDelta(pa, pb);
            double ref = referencePhaseDelta(a1, p1, a2, p2);

            assertEquals(ref, r.phaseDelta(), 1e-9,
                    "Fuzz phaseDelta mismatch at dim=" + dim + " iter=" + t);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7, 17, 127})
    @DisplayName("Fuzz: weighted phaseDelta matches amplitude-weighted reference")
    void fuzz_weightedMatchesReference(int dim) {
        Random rng = new Random(dim * 37L);
        for (int t = 0; t < 50; t++) {
            double[] a1 = new double[dim], p1 = new double[dim];
            double[] a2 = new double[dim], p2 = new double[dim];
            double[] w = new double[dim];
            for (int i = 0; i < dim; i++) {
                a1[i] = rng.nextDouble() * 2;
                p1[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
                a2[i] = rng.nextDouble() * 2;
                p2[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
                w[i] = rng.nextDouble();
            }
            WavePattern pa = new WavePattern(a1, p1);
            WavePattern pb = new WavePattern(a2, p2);
            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(w));

            ComparisonResult r = KERNEL.compareWithPhaseDelta(pa, pb, opts);
            double ref = referencePhaseDeltaWeighted(a1, p1, a2, p2, w);

            assertEquals(ref, r.phaseDelta(), 1e-9,
                    "Weighted fuzz phaseDelta mismatch at dim=" + dim + " iter=" + t);
        }
    }

    // ── Energy unchanged ──

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 127, 513})
    @DisplayName("Energy from compareWithPhaseDelta still matches compare()")
    void energyUnchanged(int dim) {
        Random rng = new Random(dim * 41L);
        for (int t = 0; t < 20; t++) {
            double[] a1 = new double[dim], p1 = new double[dim];
            double[] a2 = new double[dim], p2 = new double[dim];
            for (int i = 0; i < dim; i++) {
                a1[i] = rng.nextDouble();
                p1[i] = rng.nextDouble() * 2 * Math.PI;
                a2[i] = rng.nextDouble();
                p2[i] = rng.nextDouble() * 2 * Math.PI;
            }
            WavePattern pa = new WavePattern(a1, p1);
            WavePattern pb = new WavePattern(a2, p2);

            float expected = KERNEL.compare(pa, pb);
            ComparisonResult r = KERNEL.compareWithPhaseDelta(pa, pb);
            assertEquals(expected, r.energy(), 1e-6,
                    "Energy mismatch at dim=" + dim + " iter=" + t);
        }
    }

    // ── Boundary: single dimension ──

    @Test
    @DisplayName("Single dimension: phaseDelta = atan2(A1*A2*sin(dp), A1*A2*cos(dp)) = dp")
    void singleDimension() {
        double dp = 0.7;
        WavePattern a = new WavePattern(new double[]{2.0}, new double[]{0.0});
        WavePattern b = new WavePattern(new double[]{3.0}, new double[]{dp});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        // For 1D: theta = atan2(6*sin(0.7), 6*cos(0.7)) = 0.7
        assertEquals(dp, r.phaseDelta(), 1e-9);
    }

    @Test
    @DisplayName("Single dimension negative delta")
    void singleDimensionNegative() {
        double dp = -1.3;
        WavePattern a = new WavePattern(new double[]{1.0}, new double[]{0.5});
        WavePattern b = new WavePattern(new double[]{1.0}, new double[]{0.5 + dp});

        ComparisonResult r = KERNEL.compareWithPhaseDelta(a, b);
        assertEquals(dp, r.phaseDelta(), 1e-9);
    }
}
