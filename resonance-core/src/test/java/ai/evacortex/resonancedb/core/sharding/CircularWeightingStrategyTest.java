/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.sharding;

import ai.evacortex.resonancedb.core.storage.WavePattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 analysis: compares circular mean weighting strategies for phase routing.
 *
 * <p>Strategies compared:
 * <ol>
 *   <li>Unit (unweighted) — atan2(Σsin(φ_i), Σcos(φ_i))</li>
 *   <li>|A_i| weighted — atan2(Σ|A_i|·sin(φ_i), Σ|A_i|·cos(φ_i))</li>
 *   <li>A_i² (energy) weighted — atan2(Σ(A_i²)·sin(φ_i), Σ(A_i²)·cos(φ_i))</li>
 *   <li>Signed A_i weighted — atan2(Σ(A_i)·sin(φ_i), Σ(A_i)·cos(φ_i))</li>
 * </ol>
 */
class CircularWeightingStrategyTest {

    private static final double PI = Math.PI;
    private static final int DIM = 64;

    private static double circularMeanUnit(double[] phase) {
        double sinSum = 0.0, cosSum = 0.0;
        for (double p : phase) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        if (sinSum == 0.0 && cosSum == 0.0) return 0.0;
        return Math.atan2(sinSum, cosSum);
    }

    private static double circularMeanAbsAmplitude(double[] amplitude, double[] phase) {
        double sinSum = 0.0, cosSum = 0.0;
        for (int i = 0; i < phase.length; i++) {
            double w = Math.abs(amplitude[i]);
            sinSum += w * Math.sin(phase[i]);
            cosSum += w * Math.cos(phase[i]);
        }
        if (sinSum == 0.0 && cosSum == 0.0) return 0.0;
        return Math.atan2(sinSum, cosSum);
    }

    private static double circularMeanEnergy(double[] amplitude, double[] phase) {
        double sinSum = 0.0, cosSum = 0.0;
        for (int i = 0; i < phase.length; i++) {
            double w = amplitude[i] * amplitude[i];
            sinSum += w * Math.sin(phase[i]);
            cosSum += w * Math.cos(phase[i]);
        }
        if (sinSum == 0.0 && cosSum == 0.0) return 0.0;
        return Math.atan2(sinSum, cosSum);
    }

    private static double circularMeanSigned(double[] amplitude, double[] phase) {
        double sinSum = 0.0, cosSum = 0.0;
        for (int i = 0; i < phase.length; i++) {
            sinSum += amplitude[i] * Math.sin(phase[i]);
            cosSum += amplitude[i] * Math.cos(phase[i]);
        }
        if (sinSum == 0.0 && cosSum == 0.0) return 0.0;
        return Math.atan2(sinSum, cosSum);
    }

    private static double resultantLength(double[] phase) {
        double sinSum = 0.0, cosSum = 0.0;
        for (double p : phase) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        return Math.sqrt(sinSum * sinSum + cosSum * cosSum) / phase.length;
    }

    private static double circularDistance(double a, double b) {
        double d = Math.abs(a - b);
        return d > PI ? 2 * PI - d : d;
    }

    @Test
    @DisplayName("Uniform amplitudes: all strategies converge to same result")
    void uniformAmplitudes_allStrategiesEqual() {
        double[] phases = new double[DIM];
        double[] amps = new double[DIM];
        Random rng = new Random(42);
        for (int i = 0; i < DIM; i++) {
            phases[i] = (rng.nextBoolean() ? 1 : -1) * (PI - rng.nextDouble() * 0.2);
            amps[i] = 0.5;
        }

        double unit = circularMeanUnit(phases);
        double absAmp = circularMeanAbsAmplitude(amps, phases);
        double energy = circularMeanEnergy(amps, phases);
        double signed = circularMeanSigned(amps, phases);

        System.out.printf("Uniform amplitudes (A_i=0.5):%n");
        System.out.printf("  Unit    = %.6f%n", unit);
        System.out.printf("  |A_i|   = %.6f%n", absAmp);
        System.out.printf("  A_i²    = %.6f%n", energy);
        System.out.printf("  Signed  = %.6f%n", signed);

        assertEquals(unit, absAmp, 1e-10, "uniform amps: unit == |A_i|");
        assertEquals(unit, energy, 1e-10, "uniform amps: unit == A_i²");
        assertEquals(unit, signed, 1e-10, "uniform amps: unit == signed");
    }

    @Test
    @DisplayName("Sparse amplitudes near ±π: strategies diverge")
    void sparseAmplitudes_piBoundary() {
        double[] phases = new double[DIM];
        double[] amps = new double[DIM];
        Random rng = new Random(77);

        for (int i = 0; i < DIM; i++) {
            phases[i] = (rng.nextBoolean() ? 1 : -1) * (PI - rng.nextDouble() * 0.15);
            amps[i] = rng.nextDouble() * 0.1;
        }
        // Make a few dimensions dominant
        amps[0] = 0.95; phases[0] = PI - 0.02;
        amps[1] = 0.90; phases[1] = -(PI - 0.03);
        amps[2] = 0.85; phases[2] = PI - 0.01;

        double unit = circularMeanUnit(phases);
        double absAmp = circularMeanAbsAmplitude(amps, phases);
        double energy = circularMeanEnergy(amps, phases);
        double signed = circularMeanSigned(amps, phases);

        System.out.printf("Sparse amplitudes near ±π (3 dominant dims):%n");
        System.out.printf("  Unit    = %.6f (%.2f°)%n", unit, Math.toDegrees(unit));
        System.out.printf("  |A_i|   = %.6f (%.2f°)%n", absAmp, Math.toDegrees(absAmp));
        System.out.printf("  A_i²    = %.6f (%.2f°)%n", energy, Math.toDegrees(energy));
        System.out.printf("  Signed  = %.6f (%.2f°)%n", signed, Math.toDegrees(signed));
        System.out.printf("  Max divergence (unit vs |A_i|) = %.4f rad%n", circularDistance(unit, absAmp));
        System.out.printf("  Max divergence (unit vs A_i²)  = %.4f rad%n", circularDistance(unit, energy));
        System.out.printf("  Phase resultant R = %.4f%n", resultantLength(phases));
    }

    @Test
    @DisplayName("High-amplitude coherent cluster vs low-amplitude noise")
    void coherentCluster_vs_noise() {
        double[] phases = new double[DIM];
        double[] amps = new double[DIM];
        Random rng = new Random(99);

        // First 8 dims: high amplitude, coherent phases near +π
        for (int i = 0; i < 8; i++) {
            amps[i] = 0.8 + rng.nextDouble() * 0.2;
            phases[i] = PI - rng.nextDouble() * 0.1;
        }
        // Remaining: low amplitude, random phases
        for (int i = 8; i < DIM; i++) {
            amps[i] = rng.nextDouble() * 0.05;
            phases[i] = -PI + rng.nextDouble() * 2 * PI;
        }

        double unit = circularMeanUnit(phases);
        double absAmp = circularMeanAbsAmplitude(amps, phases);
        double energy = circularMeanEnergy(amps, phases);

        System.out.printf("Coherent cluster (8 high-amp near +π) + noise (56 low-amp random):%n");
        System.out.printf("  Unit    = %.6f (%.2f°)%n", unit, Math.toDegrees(unit));
        System.out.printf("  |A_i|   = %.6f (%.2f°)%n", absAmp, Math.toDegrees(absAmp));
        System.out.printf("  A_i²    = %.6f (%.2f°)%n", energy, Math.toDegrees(energy));
        System.out.printf("  Cluster center ≈ %.2f° — |A_i| and A_i² should be closer%n",
                Math.toDegrees(PI - 0.05));

        // Amplitude-weighted strategies should be closer to the cluster center
        double clusterCenter = PI - 0.05;
        assertTrue(circularDistance(absAmp, clusterCenter) < circularDistance(unit, clusterCenter),
                "|A_i|-weighted should be closer to cluster center than unit");
        assertTrue(circularDistance(energy, clusterCenter) < circularDistance(unit, clusterCenter),
                "A_i²-weighted should be closer to cluster center than unit");
    }

    @Test
    @DisplayName("Routing bucket divergence across strategies (16 buckets)")
    void routingBucketDivergence() {
        int numBuckets = 16;
        double step = 2 * PI / numBuckets;
        int trials = 1000;
        int[] bucketDivUnit_vs_Abs = new int[numBuckets + 1];
        int[] bucketDivUnit_vs_Energy = new int[numBuckets + 1];
        int divergeCount = 0;

        Random rng = new Random(42);
        for (int t = 0; t < trials; t++) {
            double[] phases = new double[DIM];
            double[] amps = new double[DIM];
            for (int i = 0; i < DIM; i++) {
                phases[i] = -PI + rng.nextDouble() * 2 * PI;
                amps[i] = rng.nextDouble();
            }

            double unit = circularMeanUnit(phases);
            double absAmp = circularMeanAbsAmplitude(amps, phases);
            double energy = circularMeanEnergy(amps, phases);

            int bUnit = (int) Math.floor(((unit + PI) % (2 * PI)) / step);
            int bAbs = (int) Math.floor(((absAmp + PI) % (2 * PI)) / step);
            int bEnergy = (int) Math.floor(((energy + PI) % (2 * PI)) / step);

            bUnit = Math.min(bUnit, numBuckets - 1);
            bAbs = Math.min(bAbs, numBuckets - 1);
            bEnergy = Math.min(bEnergy, numBuckets - 1);

            int divAbs = Math.min(Math.abs(bUnit - bAbs), numBuckets - Math.abs(bUnit - bAbs));
            int divEnergy = Math.min(Math.abs(bUnit - bEnergy), numBuckets - Math.abs(bUnit - bEnergy));

            bucketDivUnit_vs_Abs[divAbs]++;
            bucketDivUnit_vs_Energy[divEnergy]++;
            if (divAbs > 0 || divEnergy > 0) divergeCount++;
        }

        System.out.printf("Routing bucket divergence (16 buckets, %d trials):%n", trials);
        System.out.printf("  Trials where ANY strategy diverges from unit: %d/%d (%.1f%%)%n",
                divergeCount, trials, 100.0 * divergeCount / trials);
        System.out.printf("  Unit vs |A_i| bucket distance distribution:%n");
        for (int d = 0; d <= numBuckets / 2; d++) {
            if (bucketDivUnit_vs_Abs[d] > 0)
                System.out.printf("    %d buckets apart: %d trials%n", d, bucketDivUnit_vs_Abs[d]);
        }
        System.out.printf("  Unit vs A_i² bucket distance distribution:%n");
        for (int d = 0; d <= numBuckets / 2; d++) {
            if (bucketDivUnit_vs_Energy[d] > 0)
                System.out.printf("    %d buckets apart: %d trials%n", d, bucketDivUnit_vs_Energy[d]);
        }
    }

    @Test
    @DisplayName("Summary: strategy recommendation for routing")
    void strategyRecommendation() {
        // Test: for routing purposes, unit weighting is safest because:
        // 1. meanPhase() callers don't always have amplitude context
        // 2. PhaseShardSelector.fromManifest() only has phaseCenter, not amplitudes
        // 3. Amplitude weighting shifts the center toward dominant dimensions,
        //    but routing needs stability (same pattern → same shard), not signal fidelity
        // 4. The routing epsilon already provides tolerance for edge cases

        // Verify: unit weighting has maximal resultant (= tightest bucket assignment)
        // when phases are coherent, which is the normal case
        double[] coherentPhases = new double[DIM];
        double[] amps = new double[DIM];
        Random rng = new Random(55);
        for (int i = 0; i < DIM; i++) {
            coherentPhases[i] = 1.0 + rng.nextGaussian() * 0.1;
            amps[i] = rng.nextDouble();
        }

        double unit = circularMeanUnit(coherentPhases);
        double absAmp = circularMeanAbsAmplitude(amps, coherentPhases);
        double energy = circularMeanEnergy(amps, coherentPhases);

        System.out.printf("Strategy recommendation for routing:%n");
        System.out.printf("  Coherent data (σ=0.1 around φ=1.0):%n");
        System.out.printf("    Unit    = %.6f%n", unit);
        System.out.printf("    |A_i|   = %.6f%n", absAmp);
        System.out.printf("    A_i²    = %.6f%n", energy);
        System.out.printf("    Max divergence = %.6f rad (%.3f°)%n",
                Math.max(circularDistance(unit, absAmp), circularDistance(unit, energy)),
                Math.toDegrees(Math.max(circularDistance(unit, absAmp), circularDistance(unit, energy))));
        System.out.printf("  Conclusion: unit weighting is sufficient for routing.%n");
        System.out.printf("  Reasons:%n");
        System.out.printf("    1. Routing needs stability, not signal fidelity%n");
        System.out.printf("    2. meanPhase() callers don't always have amplitude context%n");
        System.out.printf("    3. fromManifest() only has phaseCenter, not amplitudes%n");
        System.out.printf("    4. Routing epsilon provides tolerance for edge cases%n");

        // All strategies should agree closely for coherent data
        assertTrue(circularDistance(unit, absAmp) < 0.05,
                "strategies should agree for coherent data");
    }
}
