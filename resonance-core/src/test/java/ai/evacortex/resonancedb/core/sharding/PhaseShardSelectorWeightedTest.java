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

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Iteration 033 — P13: Multi-segment Adaptive Phase Envelope validation.
 *
 * <p>Tests PhaseShardSelector.getRelevantShardsWeighted() for correctness
 * across phase buckets, boundary queries, and weighted mask types.</p>
 */
class PhaseShardSelectorWeightedTest {

    private static final double PI = Math.PI;
    private static final int DIM = 64;

    /**
     * Creates a selector with N uniform phase buckets spanning [-π, π).
     */
    private PhaseShardSelector uniformSelector(int numBuckets, double epsilon) {
        Map<Double, String> shardMap = new LinkedHashMap<>();
        double step = 2 * PI / numBuckets;
        for (int i = 0; i < numBuckets; i++) {
            double center = -PI + step * i + step / 2;
            shardMap.put(center, "seg-" + i);
        }
        return new PhaseShardSelector(shardMap, epsilon);
    }

    /** Creates a WavePattern with constant phase across all dimensions. */
    private WavePattern patternWithPhase(double phase, int dim) {
        double[] amp = new double[dim];
        double[] phases = new double[dim];
        Arrays.fill(amp, 0.5);
        Arrays.fill(phases, phase);
        return new WavePattern(amp, phases);
    }

    @Test
    void defaultWeights_usesBaseEpsilon() {
        PhaseShardSelector sel = uniformSelector(8, 0.2);
        WavePattern query = patternWithPhase(0.0, DIM);

        List<String> defaultShards = sel.getRelevantShards(query, 0.2);
        List<String> weightedShards = sel.getRelevantShardsWeighted(query, 0.2, null);

        assertEquals(defaultShards, weightedShards,
                "null profile should delegate to default getRelevantShards");

        PhaseRoutingProfile defaultProfile = PhaseRoutingProfile.from(null);
        List<String> weightedDefault = sel.getRelevantShardsWeighted(query, 0.2, defaultProfile);
        assertEquals(defaultShards, weightedDefault,
                "default profile should delegate to getRelevantShards");
    }

    @Test
    void phaseFree_returnsAllShards() {
        PhaseShardSelector sel = uniformSelector(8, 0.2);
        WavePattern query = patternWithPhase(0.0, DIM);

        CompareOptions opts = CompareOptions.withPhaseWeights(
                new PhaseWeights(new double[DIM]));
        PhaseRoutingProfile profile = PhaseRoutingProfile.from(opts);
        assertTrue(profile.isPhaseFree());

        List<String> shards = sel.getRelevantShardsWeighted(query, 0.2, profile);
        assertEquals(8, shards.size(), "phase-free must return all shards");
    }

    @Test
    void sparseWeights_expandEnvelope() {
        PhaseShardSelector sel = uniformSelector(8, 0.1);
        WavePattern query = patternWithPhase(0.0, DIM);

        PhaseRoutingProfile defaultProfile = PhaseRoutingProfile.from(null);
        List<String> narrow = sel.getRelevantShardsWeighted(query, 0.1, defaultProfile);

        double[] sparse = new double[DIM];
        sparse[0] = 1.0;
        CompareOptions sparseOpts = CompareOptions.withPhaseWeights(new PhaseWeights(sparse));
        PhaseRoutingProfile sparseProfile = PhaseRoutingProfile.from(sparseOpts);
        assertTrue(sparseProfile.routingUncertainty() > 0.5,
                "sparse mask should have high routing uncertainty");

        List<String> expanded = sel.getRelevantShardsWeighted(query, 0.1, sparseProfile);
        assertTrue(expanded.size() >= narrow.size(),
                "sparse weights should select at least as many shards as default");
    }

    @Test
    void denseWeights_minimalExpansion() {
        PhaseShardSelector sel = uniformSelector(8, 0.1);
        WavePattern query = patternWithPhase(0.0, DIM);

        double[] dense = new double[DIM];
        Arrays.fill(dense, 1.0);
        dense[0] = 0.0;
        CompareOptions denseOpts = CompareOptions.withPhaseWeights(new PhaseWeights(dense));
        PhaseRoutingProfile denseProfile = PhaseRoutingProfile.from(denseOpts);
        assertTrue(denseProfile.routingUncertainty() < 0.5,
                "dense mask should have low routing uncertainty");

        PhaseRoutingProfile defaultProfile = PhaseRoutingProfile.from(null);
        List<String> defaultShards = sel.getRelevantShardsWeighted(query, 0.1, defaultProfile);
        List<String> denseShards = sel.getRelevantShardsWeighted(query, 0.1, denseProfile);

        assertTrue(denseShards.size() <= defaultShards.size() + 2,
                "dense weights should not dramatically expand envelope");
    }

    @Test
    void queryAtBucketBoundary_includesAdjacentBuckets() {
        int numBuckets = 8;
        double bucketWidth = 2 * PI / numBuckets;
        PhaseShardSelector sel = uniformSelector(numBuckets, bucketWidth * 0.6);

        double boundary = -PI + bucketWidth;
        WavePattern query = patternWithPhase(boundary, DIM);

        List<String> shards = sel.getRelevantShards(query, bucketWidth * 0.6);
        assertTrue(shards.size() >= 2,
                "query at bucket boundary with sufficient epsilon should include adjacent buckets");
    }

    @Test
    void weightedQueryAtBoundary_expandsBeyondDefault() {
        int numBuckets = 8;
        double bucketWidth = 2 * PI / numBuckets;
        PhaseShardSelector sel = uniformSelector(numBuckets, 0.1);

        double boundary = -PI + bucketWidth;
        WavePattern query = patternWithPhase(boundary, DIM);

        double[] weights = new double[DIM];
        for (int i = 0; i < DIM; i++) weights[i] = (i % 2 == 0) ? 1.0 : 0.0;
        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(weights));
        PhaseRoutingProfile profile = PhaseRoutingProfile.from(opts);

        List<String> weighted = sel.getRelevantShardsWeighted(query, 0.1, profile);
        List<String> unweighted = sel.getRelevantShards(query, 0.1);

        assertTrue(weighted.size() >= unweighted.size(),
                "weighted boundary query should include at least as many shards");
    }

    @Test
    void queryNearPiWraparound_handledCorrectly() {
        PhaseShardSelector sel = uniformSelector(8, 0.5);

        WavePattern queryNearPi = patternWithPhase(PI - 0.1, DIM);
        List<String> shards = sel.getRelevantShards(queryNearPi, 0.5);
        assertTrue(shards.size() >= 2,
                "query near +π with ε=0.5 must wrap and include shards on both sides of ±π");

        WavePattern queryNearNegPi = patternWithPhase(-PI + 0.1, DIM);
        List<String> shardsNeg = sel.getRelevantShards(queryNearNegPi, 0.5);
        assertTrue(shardsNeg.size() >= 2,
                "query near -π with ε=0.5 must wrap and include shards on both sides of ±π");
    }

    @Test
    void weightedQueryWraparound_expandsCorrectly() {
        PhaseShardSelector sel = uniformSelector(8, 0.1);

        WavePattern query = patternWithPhase(PI - 0.05, DIM);
        double[] sparse = new double[DIM];
        sparse[0] = 1.0;
        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(sparse));
        PhaseRoutingProfile profile = PhaseRoutingProfile.from(opts);

        List<String> weighted = sel.getRelevantShardsWeighted(query, 0.1, profile);
        assertTrue(weighted.size() >= 2,
                "weighted query near π with high uncertainty should cross wraparound boundary");
    }

    @Test
    void envelopeNeverLosesRelevantShard() {
        double baseEps = 0.25;
        PhaseShardSelector sel = uniformSelector(16, baseEps);
        WavePattern query = patternWithPhase(0.3, DIM);

        double[][] masks = {
                fill(DIM, 1.0),
                denseMask(DIM, 0.9),
                adversarial(DIM),
                denseMask(DIM, 0.5),
                sparseMask(DIM, 0.1),
        };

        int prevSize = 0;
        int firstSize = -1;
        for (double[] mask : masks) {
            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(mask));
            PhaseRoutingProfile profile = PhaseRoutingProfile.from(opts);
            List<String> shards = sel.getRelevantShardsWeighted(query, baseEps, profile);
            assertTrue(shards.size() >= prevSize,
                    "more aggressive mask should not reduce envelope: got " + shards.size()
                            + " < " + prevSize);
            if (firstSize < 0) firstSize = shards.size();
            prevSize = shards.size();
        }
        assertTrue(prevSize > firstSize,
                "sparsest mask must select strictly more shards than uniform: "
                        + prevSize + " vs " + firstSize);
    }

    @Test
    void fromManifest_buildsCorrectSelector() {
        List<ManifestIndex.PatternLocation> locations = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            locations.add(new ManifestIndex.PatternLocation("seg-a", (long) i, 0.1 * (i - 5)));
        }
        for (int i = 0; i < 10; i++) {
            locations.add(new ManifestIndex.PatternLocation("seg-b", (long) i, PI / 2 + 0.1 * (i - 5)));
        }
        for (int i = 0; i < 10; i++) {
            locations.add(new ManifestIndex.PatternLocation("seg-c", (long) i, -PI / 2 + 0.1 * (i - 5)));
        }

        PhaseShardSelector sel = PhaseShardSelector.fromManifest(locations, 0.3);
        assertEquals(3, sel.allShards().size());

        WavePattern queryNearA = patternWithPhase(0.0, DIM);
        List<String> shardsA = sel.getRelevantShards(queryNearA, 0.3);
        assertTrue(shardsA.contains("seg-a"), "query near 0 should find seg-a");
    }

    private static double[] fill(int dim, double v) {
        double[] w = new double[dim];
        Arrays.fill(w, v);
        return w;
    }

    private static double[] sparseMask(int dim, double frac) {
        double[] w = new double[dim];
        int active = Math.max(1, (int)(dim * frac));
        for (int i = 0; i < active; i++) w[i] = 1.0;
        return w;
    }

    private static double[] denseMask(int dim, double frac) {
        double[] w = new double[dim];
        Arrays.fill(w, 1.0);
        int inactive = (int)(dim * (1.0 - frac));
        for (int i = 0; i < inactive; i++) w[i] = 0.0;
        return w;
    }

    private static double[] adversarial(int dim) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++) w[i] = (i % 2 == 0) ? 1.0 : 0.0;
        return w;
    }
}
