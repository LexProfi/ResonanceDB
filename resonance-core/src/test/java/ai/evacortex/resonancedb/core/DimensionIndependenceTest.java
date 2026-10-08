/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.engine.*;
import ai.evacortex.resonancedb.core.index.ResonanceMoments;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that all weighted phase routing components work correctly
 * at arbitrary (non-SIMD-aligned) dimensions.
 *
 * <p>Tests D=1,7,17,127,513 to catch hidden hardcoding, SIMD tail errors,
 * alignment assumptions, and persistence-size errors.</p>
 */
class DimensionIndependenceTest {

    private static final ResonanceKernel kernel = new JavaKernel();
    private static final long SEED = 314159L;
    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = 0.1 + rng.nextDouble() * 0.9;
            phase[i] = (rng.nextDouble() - 0.5) * 2.0 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static double[] randomWeights(Random rng, int dim) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++) w[i] = rng.nextDouble();
        return w;
    }

    private static double[] allOne(int dim) {
        double[] w = new double[dim];
        java.util.Arrays.fill(w, 1.0);
        return w;
    }

    private static double[] allZero(int dim) {
        return new double[dim];
    }

    private static double[] sparse(Random rng, int dim, double density) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++) {
            w[i] = rng.nextDouble() < density ? rng.nextDouble() : 0.0;
        }
        return w;
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("PhaseWeights: construction and properties at arbitrary dimension")
    void phaseWeightsAtDim(int dim) {
        Random rng = new Random(SEED);

        PhaseWeights pw1 = new PhaseWeights(allOne(dim));
        assertTrue(pw1.isDefault());
        assertFalse(pw1.isPhaseFree());
        assertEquals(dim, pw1.length());

        PhaseWeights pw0 = new PhaseWeights(allZero(dim));
        assertFalse(pw0.isDefault());
        assertTrue(pw0.isPhaseFree());
        assertEquals(dim, pw0.length());

        PhaseWeights pwr = new PhaseWeights(randomWeights(rng, dim));
        assertEquals(dim, pwr.length());
        pwr.validateDimension(dim);
        assertThrows(IllegalArgumentException.class, () -> pwr.validateDimension(dim + 1));
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("JavaKernel: default compare at arbitrary dimension")
    void defaultCompareAtDim(int dim) {
        Random rng = new Random(SEED);
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);

        float score = kernel.compare(a, b);
        assertTrue(score >= 0.0f && score <= 1.0f, "score out of range: " + score);
        assertEquals(1.0f, kernel.compare(a, a), 1e-5f, "self-compare should be ~1.0");
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("JavaKernel: weighted compare at arbitrary dimension")
    void weightedCompareAtDim(int dim) {
        Random rng = new Random(SEED);
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);

        CompareOptions optDefault = CompareOptions.defaultOptions();
        CompareOptions optAllOne = CompareOptions.withPhaseWeights(new PhaseWeights(allOne(dim)));
        float scoreDefault = kernel.compare(a, b, optDefault);
        float scoreAllOne = kernel.compare(a, b, optAllOne);
        assertEquals(scoreDefault, scoreAllOne, 1e-6f, "all-one must equal default");

        CompareOptions optAllZero = CompareOptions.withPhaseWeights(new PhaseWeights(allZero(dim)));
        float scoreZero = kernel.compare(a, b, optAllZero);
        assertTrue(scoreZero >= 0.0f && scoreZero <= 1.0f, "phase-free score out of range");

        CompareOptions optRandom = CompareOptions.withPhaseWeights(new PhaseWeights(randomWeights(rng, dim)));
        float scoreWeighted = kernel.compare(a, b, optRandom);
        assertTrue(scoreWeighted >= 0.0f && scoreWeighted <= 1.0f, "weighted score out of range");

        CompareOptions optSparse = CompareOptions.withPhaseWeights(new PhaseWeights(sparse(rng, dim, 0.1)));
        float scoreSparse = kernel.compare(a, b, optSparse);
        assertTrue(scoreSparse >= 0.0f && scoreSparse <= 1.0f, "sparse score out of range");
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("JavaKernel: compareWithPhaseDelta at arbitrary dimension")
    void phaseDeltaAtDim(int dim) {
        Random rng = new Random(SEED);
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);

        ComparisonResult r1 = kernel.compareWithPhaseDelta(a, b);
        assertTrue(r1.energy() >= 0.0f && r1.energy() <= 1.0f);
        assertTrue(Math.abs(r1.phaseDelta()) <= Math.PI + 0.01);

        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(randomWeights(rng, dim)));
        ComparisonResult r2 = kernel.compareWithPhaseDelta(a, b, opts);
        assertTrue(r2.energy() >= 0.0f && r2.energy() <= 1.0f);
        assertTrue(Math.abs(r2.phaseDelta()) <= Math.PI + 0.01);

        CompareOptions optFree = CompareOptions.withPhaseWeights(new PhaseWeights(allZero(dim)));
        ComparisonResult r3 = kernel.compareWithPhaseDelta(a, b, optFree);
        assertEquals(0.0, r3.phaseDelta(), 1e-10, "phase-free delta must be 0");
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("JavaKernel: compareMany at arbitrary dimension")
    void compareManyAtDim(int dim) {
        Random rng = new Random(SEED);
        WavePattern query = randomPattern(rng, dim);
        List<WavePattern> candidates = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) candidates.add(randomPattern(rng, dim));

        float[] scores = kernel.compareMany(query, candidates);
        assertEquals(20, scores.length);
        for (int i = 0; i < 20; i++) {
            assertEquals(kernel.compare(query, candidates.get(i)), scores[i], 1e-6f,
                    "compareMany[" + i + "] must match scalar compare");
        }

        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(randomWeights(rng, dim)));
        float[] wScores = kernel.compareMany(query, candidates, opts);
        for (int i = 0; i < 20; i++) {
            assertEquals(kernel.compare(query, candidates.get(i), opts), wScores[i], 1e-6f,
                    "weighted compareMany[" + i + "] must match scalar");
        }
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("ResonanceMoments: build and scoreCentroid at arbitrary dimension")
    void momentsAtDim(int dim) {
        Random rng = new Random(SEED);
        int K = 5;
        int patternsPerCentroid = 10;

        ResonanceMoments.Builder builder = new ResonanceMoments.Builder(K, dim);
        for (int c = 0; c < K; c++) {
            for (int p = 0; p < patternsPerCentroid; p++) {
                builder.addPattern(c, randomPattern(rng, dim));
            }
        }
        ResonanceMoments moments = builder.build();

        WavePattern query = randomPattern(rng, dim);

        float s1 = moments.scoreCentroid(0, query, null);
        assertTrue(Float.isFinite(s1), "default score must be finite");

        float s2 = moments.scoreCentroid(0, query, new PhaseWeights(allOne(dim)));
        assertEquals(s1, s2, 1e-5f, "all-one must equal default");

        float s3 = moments.scoreCentroid(0, query, new PhaseWeights(allZero(dim)));
        assertTrue(Float.isFinite(s3), "phase-free score must be finite");

        float s4 = moments.scoreCentroid(0, query, new PhaseWeights(randomWeights(rng, dim)));
        assertTrue(Float.isFinite(s4), "weighted score must be finite");

        int[] top = moments.topCentroidsByMoment(query, new PhaseWeights(randomWeights(new Random(SEED + 1), dim)), 3);
        assertEquals(3, top.length);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("ResonanceMoments: write/read roundtrip at arbitrary dimension")
    void momentsPersistenceAtDim(int dim, @TempDir Path tmpDir) throws IOException {
        Random rng = new Random(SEED);
        int K = 4;

        ResonanceMoments.Builder builder = new ResonanceMoments.Builder(K, dim);
        for (int c = 0; c < K; c++) {
            for (int p = 0; p < 5; p++) {
                builder.addPattern(c, randomPattern(rng, dim));
            }
        }
        ResonanceMoments original = builder.build();

        Path path = tmpDir.resolve("moments_d" + dim + ".bin");
        original.write(path);

        ResonanceMoments loaded = ResonanceMoments.load(path);
        assertNotNull(loaded);

        WavePattern query = randomPattern(new Random(SEED + 42), dim);
        PhaseWeights weights = new PhaseWeights(randomWeights(new Random(SEED + 99), dim));
        for (int c = 0; c < K; c++) {
            float orig = original.scoreCentroid(c, query, weights);
            float read = loaded.scoreCentroid(c, query, weights);
            assertEquals(orig, read, 1e-6f,
                    "centroid " + c + " score must match after persistence roundtrip");
        }

        long expectedSize = 4 + 4 + 4 + 4
                + (long) K * (3L * dim * 4 + 4 + 4)
                + 4;
        assertEquals(expectedSize, java.nio.file.Files.size(path),
                "sidecar size must be exactly header + K*(3*D*4+8) + CRC");
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("PhaseRoutingProfile: from CompareOptions at arbitrary dimension")
    void routingProfileAtDim(int dim) {
        PhaseRoutingProfile p1 = PhaseRoutingProfile.from(null);
        assertTrue(p1.isDefault());

        CompareOptions optOne = CompareOptions.withPhaseWeights(new PhaseWeights(allOne(dim)));
        PhaseRoutingProfile p2 = PhaseRoutingProfile.from(optOne);
        assertTrue(p2.isDefault(), "all-one profile should be default");

        Random rng = new Random(SEED);
        CompareOptions optW = CompareOptions.withPhaseWeights(new PhaseWeights(randomWeights(rng, dim)));
        PhaseRoutingProfile p3 = PhaseRoutingProfile.from(optW);
        assertFalse(p3.isDefault());
        assertTrue(p3.weightDrift() >= 0.0);
        assertTrue(p3.meanParticipation() >= 0.0 && p3.meanParticipation() <= 1.0);

        CompareOptions optZero = CompareOptions.withPhaseWeights(new PhaseWeights(allZero(dim)));
        PhaseRoutingProfile p4 = PhaseRoutingProfile.from(optZero);
        assertFalse(p4.isDefault());
        assertEquals(0.0, p4.meanParticipation(), 1e-10);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Determinism: identical inputs produce identical output at arbitrary dimension")
    void determinismAtDim(int dim) {
        Random rng1 = new Random(SEED);
        Random rng2 = new Random(SEED);

        WavePattern a1 = randomPattern(rng1, dim);
        WavePattern b1 = randomPattern(rng1, dim);
        WavePattern a2 = randomPattern(rng2, dim);
        WavePattern b2 = randomPattern(rng2, dim);

        double[] w1 = randomWeights(rng1, dim);
        double[] w2 = randomWeights(rng2, dim);

        CompareOptions opts1 = CompareOptions.withPhaseWeights(new PhaseWeights(w1));
        CompareOptions opts2 = CompareOptions.withPhaseWeights(new PhaseWeights(w2));

        float s1 = kernel.compare(a1, b1, opts1);
        float s2 = kernel.compare(a2, b2, opts2);
        assertEquals(s1, s2, 0.0f, "determinism: identical inputs must produce identical float output");
    }
}
