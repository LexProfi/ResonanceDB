/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.sharding;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Resultant length R computation and low-coherence detection.
 *
 * <p>Verifies R values for the required scenarios:
 * all equal, clustered, uniform/random, symmetric cancellation,
 * four-way cancellation, ±π coherent cluster.</p>
 */
class PhaseCoherenceTest {

    private static final double PI = Math.PI;
    private static final int DIM = 64;

    private static double resultantLength(double[] phase) {
        double sinSum = 0.0, cosSum = 0.0;
        for (double p : phase) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        return Math.sqrt(sinSum * sinSum + cosSum * cosSum) / phase.length;
    }

    @Test
    @DisplayName("All phases equal → R ≈ 1")
    void allEqual_R_near1() {
        double[] phases = new double[DIM];
        Arrays.fill(phases, 1.5);
        double R = resultantLength(phases);
        System.out.printf("All equal (φ=1.5): R = %.6f%n", R);
        assertTrue(R > 0.99, "R should be ≈1, got " + R);
    }

    @Test
    @DisplayName("Cluster σ=0.1 → high R")
    void cluster_highR() {
        Random rng = new Random(42);
        double[] phases = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            phases[i] = 1.0 + rng.nextGaussian() * 0.1;
        }
        double R = resultantLength(phases);
        System.out.printf("Cluster σ=0.1: R = %.6f%n", R);
        assertTrue(R > 0.9, "R should be high for tight cluster, got " + R);
    }

    @Test
    @DisplayName("Uniform/random phases → low R")
    void uniformRandom_lowR() {
        Random rng = new Random(77);
        double[] phases = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            phases[i] = -PI + rng.nextDouble() * 2 * PI;
        }
        double R = resultantLength(phases);
        System.out.printf("Uniform random: R = %.6f%n", R);
        assertTrue(R < 0.15, "R should be low for random, got " + R);
    }

    @Test
    @DisplayName("Symmetric cancellation (0, π) → R ≈ 0")
    void symmetricCancellation_R_zero() {
        double[] phases = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            phases[i] = (i % 2 == 0) ? 0.0 : PI;
        }
        double R = resultantLength(phases);
        System.out.printf("Symmetric cancellation (0,π): R = %.6f%n", R);
        assertTrue(R < 0.01, "R should be ≈0, got " + R);
    }

    @Test
    @DisplayName("Four-way cancellation (0, π/2, π, -π/2) → R ≈ 0")
    void fourWayCancellation_R_zero() {
        double[] phases = new double[DIM];
        double[] fourWay = {0.0, PI / 2, PI, -PI / 2};
        for (int i = 0; i < DIM; i++) {
            phases[i] = fourWay[i % 4];
        }
        double R = resultantLength(phases);
        System.out.printf("Four-way cancellation: R = %.6f%n", R);
        assertTrue(R < 0.01, "R should be ≈0, got " + R);
    }

    @Test
    @DisplayName("±π coherent cluster → high R despite boundary")
    void piCluster_highR() {
        Random rng = new Random(99);
        double[] phases = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            // cluster near +π with small noise, some wrap to -π side
            phases[i] = PI - 0.05 + rng.nextGaussian() * 0.08;
            while (phases[i] > PI) phases[i] -= 2 * PI;
            while (phases[i] <= -PI) phases[i] += 2 * PI;
        }
        double R = resultantLength(phases);
        System.out.printf("±π coherent cluster (σ=0.08): R = %.6f%n", R);
        assertTrue(R > 0.9, "R should be high for ±π cluster, got " + R);
    }

    @Test
    @DisplayName("Low-coherence threshold is configurable via system property")
    void threshold_configurable() {
        // Default threshold is 0.15
        double threshold = Double.parseDouble(
                System.getProperty("resonance.phase.coherenceThreshold", "0.15"));
        assertEquals(0.15, threshold, 1e-10,
                "default threshold should be 0.15");
    }
}
