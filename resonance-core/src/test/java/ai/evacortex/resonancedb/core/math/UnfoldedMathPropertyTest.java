/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.math;

import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.WavePattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property tests verifying the mathematical identities of the u-unfolding.
 *
 * <p>These tests prove that:</p>
 * <ol>
 *   <li>{@code dotFused(a,b) == dot(unfold(a), unfold(b))} — fused loop equivalence</li>
 *   <li>{@code kernel.compare(a,b) == scoreFromDot(dotFused(a,b), E(a), E(b))} — score equivalence</li>
 * </ol>
 *
 * <p>All tests use fixed seeds for reproducibility.</p>
 */
class UnfoldedMathPropertyTest {

    private static final long SEED = 42L;
    private static final int PAIR_COUNT = 10_000;
    private static final int DIM = 64; // small dim for speed; math is dimension-agnostic
    private static final double DOT_TOLERANCE = 1e-9;
    private static final double SCORE_TOLERANCE = 1e-6;

    private final ResonanceKernel kernel = new JavaKernel();

    // ─── Identity 1: dotFused ≡ dot(unfold, unfold) ──────────────────────────

    @Test
    @DisplayName("Identity 1: dotFused(a,b) == dotUnfolded(unfold(a), unfold(b)) — 10K random pairs")
    void dotFusedEquivalence() {
        Random rng = new Random(SEED);
        int failures = 0;
        double maxError = 0.0;

        for (int i = 0; i < PAIR_COUNT; i++) {
            WavePattern a = randomPattern(rng, DIM);
            WavePattern b = randomPattern(rng, DIM);

            double fused = UnfoldedMath.dotFused(a, b);
            double[] ua = UnfoldedMath.unfold(a);
            double[] ub = UnfoldedMath.unfold(b);
            double unfolded = UnfoldedMath.dotUnfolded(ua, ub);

            double error = Math.abs(fused - unfolded);
            maxError = Math.max(maxError, error);
            if (error > DOT_TOLERANCE) {
                failures++;
            }
        }

        assertEquals(0, failures,
                "dotFused vs dotUnfolded: " + failures + " failures out of " + PAIR_COUNT +
                        ", max error = " + maxError);
    }

    @Test
    @DisplayName("Identity 1 (flat variant): dotFusedFlat == dotUnfolded(unfold, unfold)")
    void dotFusedFlatEquivalence() {
        Random rng = new Random(SEED + 1);

        for (int i = 0; i < PAIR_COUNT; i++) {
            WavePattern a = randomPattern(rng, DIM);
            WavePattern b = randomPattern(rng, DIM);

            double flat = UnfoldedMath.dotFusedFlat(
                    a.amplitude(), a.phase(), b.amplitude(), b.phase(), DIM);
            double fused = UnfoldedMath.dotFused(a, b);

            assertEquals(fused, flat, DOT_TOLERANCE,
                    "dotFusedFlat vs dotFused mismatch at pair " + i);
        }
    }

    // ─── Identity 2: kernel.compare ≡ scoreFromDot ───────────────────────────

    @Test
    @DisplayName("Identity 2: kernel.compare(a,b) == scoreFromDot(dotFused(a,b), E(a), E(b)) — 10K pairs")
    void scoreEquivalence() {
        Random rng = new Random(SEED);
        int failures = 0;
        double maxError = 0.0;

        for (int i = 0; i < PAIR_COUNT; i++) {
            WavePattern a = randomPattern(rng, DIM);
            WavePattern b = randomPattern(rng, DIM);

            float kernelScore = kernel.compare(a, b);

            double dot = UnfoldedMath.dotFused(a, b);
            double e1 = UnfoldedMath.energy(a);
            double e2 = UnfoldedMath.energy(b);
            float uScore = UnfoldedMath.scoreFromDot(dot, e1, e2);

            double error = Math.abs(kernelScore - uScore);
            maxError = Math.max(maxError, error);
            if (error > SCORE_TOLERANCE) {
                failures++;
            }
        }

        assertEquals(0, failures,
                "kernel.compare vs scoreFromDot: " + failures + " failures out of " + PAIR_COUNT +
                        ", max error = " + maxError);
    }

    // ─── Edge cases ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("Zero amplitude patterns: energy=0, dot=0, score=0")
    void zeroAmplitude() {
        WavePattern zero = new WavePattern(new double[DIM], new double[DIM]);
        assertEquals(0.0, UnfoldedMath.energy(zero));
        assertEquals(0.0, UnfoldedMath.dotFused(zero, zero));
        assertEquals(0.0f, UnfoldedMath.scoreFromDot(0.0, 0.0, 0.0));

        double[] u = UnfoldedMath.unfold(zero);
        assertEquals(2 * DIM, u.length);
        for (double v : u) {
            assertEquals(0.0, v);
        }
    }

    @Test
    @DisplayName("Identical patterns: score == kernel.compare(a, a)")
    void identicalPatterns() {
        Random rng = new Random(SEED + 2);
        for (int i = 0; i < 100; i++) {
            WavePattern a = randomPattern(rng, DIM);
            float kernelScore = kernel.compare(a, a);

            double dot = UnfoldedMath.dotFused(a, a);
            double e = UnfoldedMath.energy(a);
            float uScore = UnfoldedMath.scoreFromDot(dot, e, e);

            assertEquals(kernelScore, uScore, SCORE_TOLERANCE,
                    "Self-comparison mismatch at pattern " + i);
        }
    }

    @Test
    @DisplayName("Opposite phase: cos(π) = -1, minimal interference")
    void oppositePhase() {
        double[] amp = new double[DIM];
        double[] phase0 = new double[DIM];
        double[] phasePi = new double[DIM];
        java.util.Arrays.fill(amp, 1.0);
        java.util.Arrays.fill(phasePi, Math.PI);

        WavePattern a = new WavePattern(amp, phase0);
        WavePattern b = new WavePattern(amp, phasePi);

        double dot = UnfoldedMath.dotFused(a, b);
        assertTrue(dot < 0, "Opposite phase should produce negative dot: " + dot);

        float score = UnfoldedMath.scoreFromDot(dot, UnfoldedMath.energy(a), UnfoldedMath.energy(b));
        assertTrue(score < 0.01f, "Opposite phase should produce near-zero score: " + score);
    }

    @Test
    @DisplayName("Symmetry: dotFused(a,b) == dotFused(b,a)")
    void symmetry() {
        Random rng = new Random(SEED + 3);
        for (int i = 0; i < 1000; i++) {
            WavePattern a = randomPattern(rng, DIM);
            WavePattern b = randomPattern(rng, DIM);
            assertEquals(UnfoldedMath.dotFused(a, b), UnfoldedMath.dotFused(b, a), DOT_TOLERANCE,
                    "Symmetry violated at pair " + i);
        }
    }

    @Test
    @DisplayName("Unfold dimension: unfold(dim=n) produces 2n-dimensional vector")
    void unfoldDimension() {
        for (int n : new int[]{1, 4, 64, 128, 1536}) {
            WavePattern p = new WavePattern(new double[n], new double[n]);
            double[] u = UnfoldedMath.unfold(p);
            assertEquals(2 * n, u.length, "Unfold dimension for n=" + n);
        }
    }

    @Test
    @DisplayName("Energy: energy(p) == ‖unfold(p)‖²")
    void energyEqualsUnfoldedNormSquared() {
        Random rng = new Random(SEED + 4);
        for (int i = 0; i < 1000; i++) {
            WavePattern p = randomPattern(rng, DIM);
            double energy = UnfoldedMath.energy(p);
            double[] u = UnfoldedMath.unfold(p);
            double normSq = UnfoldedMath.dotUnfolded(u, u);
            assertEquals(energy, normSq, DOT_TOLERANCE,
                    "Energy vs ‖u‖² mismatch at pattern " + i);
        }
    }

    @Test
    @DisplayName("L2 normalize: result has unit norm")
    void l2NormalizeUnit() {
        Random rng = new Random(SEED + 5);
        for (int i = 0; i < 100; i++) {
            WavePattern p = randomPattern(rng, DIM);
            double[] u = UnfoldedMath.unfold(p);
            UnfoldedMath.l2Normalize(u);
            double norm = Math.sqrt(UnfoldedMath.dotUnfolded(u, u));
            assertEquals(1.0, norm, 1e-12, "Normalized vector should have unit norm");
        }
    }

    @Test
    @DisplayName("Large dimension (dim=1536): identities hold at production scale")
    void largeDimension() {
        Random rng = new Random(SEED + 6);
        int prodDim = 1536;

        for (int i = 0; i < 100; i++) {
            WavePattern a = randomPattern(rng, prodDim);
            WavePattern b = randomPattern(rng, prodDim);

            // Identity 1
            double fused = UnfoldedMath.dotFused(a, b);
            double unfolded = UnfoldedMath.dotUnfolded(UnfoldedMath.unfold(a), UnfoldedMath.unfold(b));
            assertEquals(fused, unfolded, DOT_TOLERANCE,
                    "dotFused vs dotUnfolded at dim=1536, pair " + i);

            // Identity 2
            float kernelScore = kernel.compare(a, b);
            float uScore = UnfoldedMath.scoreFromDot(fused,
                    UnfoldedMath.energy(a), UnfoldedMath.energy(b));
            assertEquals(kernelScore, uScore, SCORE_TOLERANCE,
                    "kernel vs scoreFromDot at dim=1536, pair " + i);
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }
}
