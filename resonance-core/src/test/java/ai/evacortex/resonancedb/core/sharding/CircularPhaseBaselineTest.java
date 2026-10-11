/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.sharding;

import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.PhaseRoutingProfile;
import ai.evacortex.resonancedb.core.engine.PhaseWeights;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;
import ai.evacortex.resonancedb.core.storage.WavePattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduces boundary cases where arithmetic mean
 * diverges from circular mean, and measures impact on shard selection.
 */
class CircularPhaseBaselineTest {

    private static final double PI = Math.PI;
    private static final int DIM = 64;

    private static double arithmeticMeanPhase(double[] phase) {
        double sum = 0.0;
        for (double v : phase) sum += v;
        return sum / phase.length;
    }

    private static double circularMeanPhase(double[] phase) {
        double sinSum = 0.0, cosSum = 0.0;
        for (double p : phase) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        return Math.atan2(sinSum, cosSum);
    }

    private static double circularDistance(double a, double b) {
        double d = Math.abs(a - b);
        return d > PI ? 2 * PI - d : d;
    }

    private static WavePattern patternWithPhases(double[] phases) {
        double[] amp = new double[phases.length];
        Arrays.fill(amp, 0.5);
        return new WavePattern(amp, phases);
    }

    @Test
    @DisplayName("Boundary: phases near ±π — arithmetic mean collapses to ~0, circular stays near ±π")
    void nearPiBoundary_arithmeticVsCircular() {
        double[] phases = new double[DIM];
        Random rng = new Random(42);
        for (int i = 0; i < DIM; i++) {
            phases[i] = (rng.nextBoolean() ? 1 : -1) * (PI - rng.nextDouble() * 0.2);
        }

        double arith = arithmeticMeanPhase(phases);
        double circ = circularMeanPhase(phases);

        System.out.printf("Near ±π boundary:%n");
        System.out.printf("  Arithmetic mean = %.4f%n", arith);
        System.out.printf("  Circular mean   = %.4f%n", circ);
        System.out.printf("  Divergence      = %.4f rad%n", circularDistance(arith, circ));

        assertTrue(Math.abs(circ) > 2.0,
                "circular mean should stay near ±π, got " + circ);
        assertTrue(Math.abs(arith) < 1.0,
                "arithmetic mean should collapse toward 0, got " + arith);
    }

    @Test
    @DisplayName("Boundary: 179° and -179° — arithmetic ≈ 0°, circular ≈ ±180°")
    void deg179_and_neg179() {
        double p179 = Math.toRadians(179);
        double pNeg179 = Math.toRadians(-179);
        double[] phases = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            phases[i] = (i % 2 == 0) ? p179 : pNeg179;
        }

        double arith = arithmeticMeanPhase(phases);
        double circ = circularMeanPhase(phases);

        System.out.printf("179°/-179° test:%n");
        System.out.printf("  Arithmetic mean = %.4f (%.1f°)%n", arith, Math.toDegrees(arith));
        System.out.printf("  Circular mean   = %.4f (%.1f°)%n", circ, Math.toDegrees(circ));

        assertTrue(Math.abs(arith) < 0.1, "arithmetic should collapse near 0");
        assertTrue(Math.abs(circ) > PI - 0.1, "circular should stay near ±π");
    }

    @Test
    @DisplayName("Boundary: 359° and 1° — arithmetic ≈ 180°, circular ≈ 0°")
    void deg359_and_deg1() {
        double p359 = Math.toRadians(359) - PI;
        double p1 = Math.toRadians(1) - PI;
        double[] phases = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            phases[i] = (i % 2 == 0) ? p359 : p1;
        }

        double arith = arithmeticMeanPhase(phases);
        double circ = circularMeanPhase(phases);

        System.out.printf("359°/1° (shifted to [-π,π]):%n");
        System.out.printf("  Arithmetic mean = %.4f (%.1f°)%n", arith, Math.toDegrees(arith));
        System.out.printf("  Circular mean   = %.4f (%.1f°)%n", circ, Math.toDegrees(circ));

        double circDist = circularDistance(arith, circ);
        System.out.printf("  Divergence      = %.4f rad (%.1f°)%n", circDist, Math.toDegrees(circDist));

        assertTrue(circDist > 0.1,
                "359°/1° boundary should produce significant divergence, got " + circDist + " rad");
    }

    @Test
    @DisplayName("Shard routing divergence: pattern near ±π boundary hits wrong shard with arithmetic mean")
    void shardRoutingDivergence() {
        int numBuckets = 16;
        Map<Double, String> shardMap = new LinkedHashMap<>();
        double step = 2 * PI / numBuckets;
        for (int i = 0; i < numBuckets; i++) {
            double center = -PI + step * i + step / 2;
            shardMap.put(center, "seg-" + i);
        }
        PhaseShardSelector sel = new PhaseShardSelector(shardMap, step * 0.6);

        double[] phases = new double[DIM];
        Random rng = new Random(123);
        for (int i = 0; i < DIM; i++) {
            phases[i] = (rng.nextBoolean() ? 1 : -1) * (PI - rng.nextDouble() * 0.1);
        }
        WavePattern query = patternWithPhases(phases);

        double arithCenter = arithmeticMeanPhase(phases);
        double circCenter = circularMeanPhase(phases);

        List<String> selectedShards = sel.getRelevantShards(query, step * 0.6);

        int arithBucket = (int) Math.floor((arithCenter + PI) / step);
        int circBucket = (int) Math.floor(((circCenter + 2 * PI) % (2 * PI)) / step);

        System.out.printf("Shard routing test (16 buckets):%n");
        System.out.printf("  Arithmetic center = %.4f → bucket %d%n", arithCenter, arithBucket);
        System.out.printf("  Circular center   = %.4f → bucket %d%n", circCenter, circBucket);
        System.out.printf("  Selected shards   = %s%n", selectedShards);
        System.out.printf("  Bucket divergence = %d buckets apart%n",
                Math.min(Math.abs(arithBucket - circBucket),
                        numBuckets - Math.abs(arithBucket - circBucket)));

        assertNotEquals(arithBucket, circBucket,
                "boundary case should produce different buckets for arithmetic vs circular");
    }

    @Test
    @DisplayName("Phase overflow: linear distance falsely triggers for phases near ±π")
    void phaseOverflow_linearVsCircular() {
        double existingCenter = PI - 0.05;
        double newPhaseCenter = -PI + 0.05;

        double linearDist = Math.abs(newPhaseCenter - existingCenter);
        double circDist = circularDistance(newPhaseCenter, existingCenter);

        System.out.printf("Phase overflow detection:%n");
        System.out.printf("  Existing center = %.4f%n", existingCenter);
        System.out.printf("  New center      = %.4f%n", newPhaseCenter);
        System.out.printf("  Linear distance = %.4f (overflow=%b)%n", linearDist, linearDist > 0.15);
        System.out.printf("  Circular dist   = %.4f (overflow=%b)%n", circDist, circDist > 0.15);

        assertTrue(linearDist > 0.15, "linear distance should trigger overflow (false positive)");
        assertTrue(circDist < 0.15, "circular distance should NOT trigger overflow");
    }

    @Test
    @DisplayName("fromManifest: per-shard center with patterns near ±π boundary")
    void fromManifest_piBoundary() {
        List<ManifestIndex.PatternLocation> locations = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            double phase = PI - 0.05 + (i % 2 == 0 ? 0.0 : -2 * PI + 0.1);
            locations.add(new ManifestIndex.PatternLocation("seg-boundary", (long) i, phase));
        }

        PhaseShardSelector sel = PhaseShardSelector.fromManifest(locations, 0.3);

        List<String> shards = sel.allShards();
        assertEquals(1, shards.size());

        double[] phases = new double[DIM];
        Arrays.fill(phases, PI - 0.02);
        WavePattern query = patternWithPhases(phases);
        List<String> found = sel.getRelevantShards(query, 0.3);
        System.out.printf("fromManifest ±π boundary: shards=%s, query found=%s%n", shards, found);
        assertFalse(found.isEmpty(), "query near ±π should find the boundary shard");
    }

    @Test
    @DisplayName("Identical phases: arithmetic == circular (no divergence)")
    void identicalPhases_noDivergence() {
        double[] phases = new double[DIM];
        Arrays.fill(phases, 1.5);

        double arith = arithmeticMeanPhase(phases);
        double circ = circularMeanPhase(phases);

        assertEquals(arith, circ, 1e-10, "identical phases should give same result");
    }

    @Test
    @DisplayName("Opposite phases with full cancellation: circular mean undefined, arithmetic = 0")
    void oppositePhases_cancellation() {
        double[] phases = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            phases[i] = (i % 2 == 0) ? 0.0 : PI;
        }

        double arith = arithmeticMeanPhase(phases);
        double circ = circularMeanPhase(phases);

        double sinSum = 0.0, cosSum = 0.0;
        for (double p : phases) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        double R = Math.sqrt(sinSum * sinSum + cosSum * cosSum) / DIM;

        System.out.printf("Full cancellation:%n");
        System.out.printf("  Arithmetic = %.4f%n", arith);
        System.out.printf("  Circular   = %.4f%n", circ);
        System.out.printf("  Resultant  = %.6f%n", R);

        assertTrue(R < 0.01, "resultant length should be ≈0 for full cancellation, got " + R);
    }

    @Test
    @DisplayName("Low-coherence: random uniform phases — both means unreliable")
    void randomUniformPhases_lowCoherence() {
        Random rng = new Random(77);
        double[] phases = new double[DIM];
        for (int i = 0; i < DIM; i++) {
            phases[i] = -PI + rng.nextDouble() * 2 * PI;
        }

        double arith = arithmeticMeanPhase(phases);
        double circ = circularMeanPhase(phases);

        double sinSum = 0, cosSum = 0;
        for (double p : phases) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        double R = Math.sqrt(sinSum * sinSum + cosSum * cosSum) / DIM;

        System.out.printf("Low coherence (uniform random):%n");
        System.out.printf("  Arithmetic = %.4f%n", arith);
        System.out.printf("  Circular   = %.4f%n", circ);
        System.out.printf("  Resultant R = %.4f (low = unreliable mean)%n", R);

        assertTrue(R < 0.3, "resultant length should be low for uniform random");
    }

    @Test
    @DisplayName("Weighted phase center: sparse weights near ±π boundary")
    void weightedPhaseCenter_sparseBoundary() {
        double[] phases = new double[DIM];
        Random rng = new Random(99);
        for (int i = 0; i < DIM; i++) {
            phases[i] = (rng.nextBoolean() ? 1 : -1) * (PI - rng.nextDouble() * 0.15);
        }
        WavePattern query = patternWithPhases(phases);

        double[] sparseW = new double[DIM];
        sparseW[0] = 1.0;
        sparseW[1] = 1.0;
        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(sparseW));
        PhaseRoutingProfile profile = PhaseRoutingProfile.from(opts);

        double weighted = profile.weightedPhaseCenter(query);

        double expectedCirc = Math.atan2(
                sparseW[0] * Math.sin(phases[0]) + sparseW[1] * Math.sin(phases[1]),
                sparseW[0] * Math.cos(phases[0]) + sparseW[1] * Math.cos(phases[1])
        );

        System.out.printf("Weighted phase center (sparse, ±π boundary):%n");
        System.out.printf("  Weighted center = %.4f%n", weighted);
        System.out.printf("  Expected        = %.4f%n", expectedCirc);
        System.out.printf("  Divergence      = %.4f rad%n", circularDistance(weighted, expectedCirc));

        assertEquals(expectedCirc, weighted, 1e-10,
                "weighted phase center should match manual circular computation");
    }
}
