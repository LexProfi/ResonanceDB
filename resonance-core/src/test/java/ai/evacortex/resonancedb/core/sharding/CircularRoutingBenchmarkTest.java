/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.sharding;

import ai.evacortex.resonancedb.core.storage.ManifestIndex;
import ai.evacortex.resonancedb.core.storage.WavePattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 4: Benchmark — routing correctness of circular mean vs arithmetic mean.
 *
 * <p>Measures routing divergence: how often the two strategies select different shards,
 * and whether circular routing correctly groups ±π-boundary patterns.</p>
 */
class CircularRoutingBenchmarkTest {

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
        if (sinSum == 0.0 && cosSum == 0.0) return 0.0;
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
    @DisplayName("Routing divergence: arithmetic vs circular on random corpus (16 shards)")
    void routingDivergence_randomCorpus() {
        int numShards = 16;
        double step = 2 * PI / numShards;
        int corpusSize = 5000;
        Random rng = new Random(42);

        int divergeCount = 0;
        int boundaryCount = 0;

        for (int i = 0; i < corpusSize; i++) {
            double[] phases = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                phases[d] = -PI + rng.nextDouble() * 2 * PI;
            }

            double arith = arithmeticMeanPhase(phases);
            double circ = circularMeanPhase(phases);

            int arithBucket = (int) Math.floor(((arith + PI) % (2 * PI)) / step);
            int circBucket = (int) Math.floor(((circ + 2 * PI) % (2 * PI)) / step);
            arithBucket = Math.min(arithBucket, numShards - 1);
            circBucket = Math.min(circBucket, numShards - 1);

            if (arithBucket != circBucket) divergeCount++;
            if (Math.abs(circ) > PI - 0.3) boundaryCount++;
        }

        double divergeRate = 100.0 * divergeCount / corpusSize;
        System.out.printf("Routing divergence (random corpus, %d patterns, %d shards):%n",
                corpusSize, numShards);
        System.out.printf("  Divergent routing: %d/%d (%.1f%%)%n", divergeCount, corpusSize, divergeRate);
        System.out.printf("  Boundary patterns (|φ̄| > π-0.3): %d/%d (%.1f%%)%n",
                boundaryCount, corpusSize, 100.0 * boundaryCount / corpusSize);
    }

    @Test
    @DisplayName("Routing divergence: boundary-heavy corpus (phases clustered near ±π)")
    void routingDivergence_boundaryCorpus() {
        int numShards = 16;
        double step = 2 * PI / numShards;
        int corpusSize = 2000;
        Random rng = new Random(77);

        int divergeCount = 0;
        int[] bucketDivDistribution = new int[numShards / 2 + 1];

        for (int i = 0; i < corpusSize; i++) {
            double[] phases = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                // Generate phases near ±π boundary
                phases[d] = (rng.nextBoolean() ? 1 : -1) * (PI - rng.nextDouble() * 0.2);
            }

            double arith = arithmeticMeanPhase(phases);
            double circ = circularMeanPhase(phases);

            int arithBucket = (int) Math.floor(((arith + PI) % (2 * PI)) / step);
            int circBucket = (int) Math.floor(((circ + 2 * PI) % (2 * PI)) / step);
            arithBucket = Math.min(arithBucket, numShards - 1);
            circBucket = Math.min(circBucket, numShards - 1);

            if (arithBucket != circBucket) divergeCount++;
            int div = Math.min(Math.abs(arithBucket - circBucket),
                    numShards - Math.abs(arithBucket - circBucket));
            bucketDivDistribution[Math.min(div, numShards / 2)]++;
        }

        double divergeRate = 100.0 * divergeCount / corpusSize;
        System.out.printf("Routing divergence (±π boundary corpus, %d patterns, %d shards):%n",
                corpusSize, numShards);
        System.out.printf("  Divergent routing: %d/%d (%.1f%%)%n", divergeCount, corpusSize, divergeRate);
        System.out.printf("  Bucket distance distribution:%n");
        for (int d = 0; d <= numShards / 2; d++) {
            if (bucketDivDistribution[d] > 0) {
                System.out.printf("    %d buckets apart: %d patterns%n", d, bucketDivDistribution[d]);
            }
        }

        // For boundary corpus, arithmetic should diverge significantly from circular
        assertTrue(divergeRate > 50.0,
                "boundary corpus should show >50% divergence, got " + divergeRate);
    }

    @Test
    @DisplayName("Phase overflow false positive rate: linear vs circular distance")
    void phaseOverflow_falsePositiveRate() {
        int corpusSize = 10000;
        Random rng = new Random(123);

        int linearFP = 0;
        int circularFP = 0;
        int trueOverflow = 0;

        for (int i = 0; i < corpusSize; i++) {
            // Simulate existing center near ±π and new pattern nearby
            double existing = (rng.nextBoolean() ? 1 : -1) * (PI - rng.nextDouble() * 0.1);
            double newPhase = existing + (rng.nextGaussian() * 0.05);
            // Wrap to [-π, π]
            while (newPhase > PI) newPhase -= 2 * PI;
            while (newPhase <= -PI) newPhase += 2 * PI;

            double linearDist = Math.abs(newPhase - existing);
            double circDist = circularDistance(newPhase, existing);

            boolean linearOverflow = linearDist > 0.15;
            boolean circularOverflow = circDist > 0.15;

            if (linearOverflow && !circularOverflow) linearFP++;
            if (circularOverflow) trueOverflow++;
        }

        double fpRate = 100.0 * linearFP / corpusSize;
        System.out.printf("Phase overflow false positive rate (%d trials):%n", corpusSize);
        System.out.printf("  Linear false positives: %d/%d (%.1f%%)%n", linearFP, corpusSize, fpRate);
        System.out.printf("  True overflows (circular): %d/%d (%.1f%%)%n",
                trueOverflow, corpusSize, 100.0 * trueOverflow / corpusSize);
        System.out.printf("  Impact: %d unnecessary segment creations avoided%n", linearFP);

        // Linear should have significantly more false positives near ±π
        assertTrue(linearFP > 100, "should detect >100 false positives, got " + linearFP);
    }

    @Test
    @DisplayName("fromManifest: circular vs arithmetic center on boundary data")
    void fromManifest_circularVsArithmetic() {
        // Build a manifest with patterns split across ±π boundary
        List<ManifestIndex.PatternLocation> locations = new ArrayList<>();
        Random rng = new Random(42);

        // Shard A: patterns near +π
        for (int i = 0; i < 100; i++) {
            double phase = PI - rng.nextDouble() * 0.1;
            locations.add(new ManifestIndex.PatternLocation("shard-A", (long) i, phase));
        }
        // Shard A also: patterns near -π (same shard, should cluster together)
        for (int i = 100; i < 200; i++) {
            double phase = -(PI - rng.nextDouble() * 0.1);
            locations.add(new ManifestIndex.PatternLocation("shard-A", (long) i, phase));
        }
        // Shard B: patterns near 0
        for (int i = 200; i < 400; i++) {
            double phase = rng.nextGaussian() * 0.1;
            locations.add(new ManifestIndex.PatternLocation("shard-B", (long) i, phase));
        }

        PhaseShardSelector sel = PhaseShardSelector.fromManifest(locations, 0.3);

        // Query near +π — should find shard-A
        double[] qPhases = new double[DIM];
        Arrays.fill(qPhases, PI - 0.05);
        WavePattern queryPi = patternWithPhases(qPhases);
        List<String> foundPi = sel.getRelevantShards(queryPi, 0.3);

        // Query near -π — should also find shard-A (circular wrapping)
        Arrays.fill(qPhases, -(PI - 0.05));
        WavePattern queryNegPi = patternWithPhases(qPhases);
        List<String> foundNegPi = sel.getRelevantShards(queryNegPi, 0.3);

        // Query near 0 — should find shard-B
        Arrays.fill(qPhases, 0.05);
        WavePattern queryZero = patternWithPhases(qPhases);
        List<String> foundZero = sel.getRelevantShards(queryZero, 0.3);

        System.out.printf("fromManifest circular center test:%n");
        System.out.printf("  Query near +π: found %s%n", foundPi);
        System.out.printf("  Query near -π: found %s%n", foundNegPi);
        System.out.printf("  Query near  0: found %s%n", foundZero);

        assertTrue(foundPi.contains("shard-A"), "query near +π should find shard-A");
        assertTrue(foundNegPi.contains("shard-A"), "query near -π should find shard-A");
        assertTrue(foundZero.contains("shard-B"), "query near 0 should find shard-B");
    }
}
