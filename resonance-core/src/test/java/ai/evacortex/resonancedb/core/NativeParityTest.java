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
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies mathematical parity between JavaKernel and NativeKernel
 * across all three scoring paths (default, weighted, phase-free)
 * at arbitrary dimensions.
 *
 * <p>Contract: max absolute error ≤ 1e-5 for all paths.</p>
 */
class NativeParityTest {

    private static final float EPSILON = 1e-5f;
    private static final long SEED = 271828L;
    private static final int BATCH_SIZE = 30;

    private static final JavaKernel JAVA = new JavaKernel();
    private static ResonanceKernel NATIVE;

    @BeforeAll
    static void loadNative() {
        try {
            NATIVE = new NativeKernel();
        } catch (Throwable t) {
            NATIVE = null;
        }
    }

    private void requireNative() {
        assumeTrue(NATIVE != null, "NativeKernel not available — skipping");
    }

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

    private static double[] sparseWeights(Random rng, int dim, double density) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++)
            w[i] = rng.nextDouble() < density ? rng.nextDouble() : 0.0;
        return w;
    }

    private static double[] allZero(int dim) {
        return new double[dim];
    }

    private static double[] allOne(int dim) {
        double[] w = new double[dim];
        java.util.Arrays.fill(w, 1.0);
        return w;
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: scalar compare — default path")
    void scalarCompareDefault(int dim) {
        requireNative();
        Random rng = new Random(SEED);
        float maxErr = 0f;

        for (int trial = 0; trial < 100; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);

            float jScore = JAVA.compare(a, b);
            float nScore = NATIVE.compare(a, b);
            float err = Math.abs(jScore - nScore);
            maxErr = Math.max(maxErr, err);
            assertTrue(err <= EPSILON,
                    "Default compare trial=" + trial + " java=" + jScore + " native=" + nScore + " err=" + err);
        }
        System.out.printf("  D=%d default scalar maxErr=%.2e%n", dim, maxErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: scalar compare — weighted path")
    void scalarCompareWeighted(int dim) {
        requireNative();
        Random rng = new Random(SEED);
        float maxErr = 0f;

        for (int trial = 0; trial < 100; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);
            double[] w = randomWeights(rng, dim);
            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(w));

            float jScore = JAVA.compare(a, b, opts);
            float nScore = NATIVE.compare(a, b, opts);
            float err = Math.abs(jScore - nScore);
            maxErr = Math.max(maxErr, err);
            assertTrue(err <= EPSILON,
                    "Weighted compare trial=" + trial + " java=" + jScore + " native=" + nScore + " err=" + err);
        }
        System.out.printf("  D=%d weighted scalar maxErr=%.2e%n", dim, maxErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: scalar compare — phase-free path")
    void scalarComparePhaseFree(int dim) {
        requireNative();
        Random rng = new Random(SEED);
        float maxErr = 0f;

        for (int trial = 0; trial < 100; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);
            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(allZero(dim)));

            float jScore = JAVA.compare(a, b, opts);
            float nScore = NATIVE.compare(a, b, opts);
            float err = Math.abs(jScore - nScore);
            maxErr = Math.max(maxErr, err);
            assertTrue(err <= EPSILON,
                    "Phase-free compare trial=" + trial + " java=" + jScore + " native=" + nScore + " err=" + err);
        }
        System.out.printf("  D=%d phase-free scalar maxErr=%.2e%n", dim, maxErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: scalar compare — sparse weights")
    void scalarCompareSparse(int dim) {
        requireNative();
        Random rng = new Random(SEED);
        float maxErr = 0f;

        for (int trial = 0; trial < 100; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);
            double[] w = sparseWeights(rng, dim, 0.1);
            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(w));

            float jScore = JAVA.compare(a, b, opts);
            float nScore = NATIVE.compare(a, b, opts);
            float err = Math.abs(jScore - nScore);
            maxErr = Math.max(maxErr, err);
            assertTrue(err <= EPSILON,
                    "Sparse compare trial=" + trial + " java=" + jScore + " native=" + nScore + " err=" + err);
        }
        System.out.printf("  D=%d sparse scalar maxErr=%.2e%n", dim, maxErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: compareMany — default path")
    void compareManyDefault(int dim) {
        requireNative();
        Random rng = new Random(SEED);

        WavePattern query = randomPattern(rng, dim);
        List<WavePattern> candidates = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) candidates.add(randomPattern(rng, dim));

        float[] jScores = JAVA.compareMany(query, candidates);
        float[] nScores = NATIVE.compareMany(query, candidates);

        assertEquals(jScores.length, nScores.length);
        float maxErr = 0f;
        for (int i = 0; i < jScores.length; i++) {
            float err = Math.abs(jScores[i] - nScores[i]);
            maxErr = Math.max(maxErr, err);
            assertTrue(err <= EPSILON,
                    "Default compareMany[" + i + "] java=" + jScores[i] + " native=" + nScores[i] + " err=" + err);
        }
        System.out.printf("  D=%d default batch maxErr=%.2e%n", dim, maxErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: compareMany — weighted path")
    void compareManyWeighted(int dim) {
        requireNative();
        Random rng = new Random(SEED);

        WavePattern query = randomPattern(rng, dim);
        List<WavePattern> candidates = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) candidates.add(randomPattern(rng, dim));
        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(randomWeights(rng, dim)));

        float[] jScores = JAVA.compareMany(query, candidates, opts);
        float[] nScores = NATIVE.compareMany(query, candidates, opts);

        assertEquals(jScores.length, nScores.length);
        float maxErr = 0f;
        for (int i = 0; i < jScores.length; i++) {
            float err = Math.abs(jScores[i] - nScores[i]);
            maxErr = Math.max(maxErr, err);
            assertTrue(err <= EPSILON,
                    "Weighted compareMany[" + i + "] java=" + jScores[i] + " native=" + nScores[i] + " err=" + err);
        }
        System.out.printf("  D=%d weighted batch maxErr=%.2e%n", dim, maxErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: compareMany — phase-free path")
    void compareManyPhaseFree(int dim) {
        requireNative();
        Random rng = new Random(SEED);

        WavePattern query = randomPattern(rng, dim);
        List<WavePattern> candidates = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) candidates.add(randomPattern(rng, dim));
        CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(allZero(dim)));

        float[] jScores = JAVA.compareMany(query, candidates, opts);
        float[] nScores = NATIVE.compareMany(query, candidates, opts);

        assertEquals(jScores.length, nScores.length);
        float maxErr = 0f;
        for (int i = 0; i < jScores.length; i++) {
            float err = Math.abs(jScores[i] - nScores[i]);
            maxErr = Math.max(maxErr, err);
            assertTrue(err <= EPSILON,
                    "Phase-free compareMany[" + i + "] java=" + jScores[i] + " native=" + nScores[i] + " err=" + err);
        }
        System.out.printf("  D=%d phase-free batch maxErr=%.2e%n", dim, maxErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: compareWithPhaseDelta — default path")
    void phaseDeltaDefault(int dim) {
        requireNative();
        Random rng = new Random(SEED);
        float maxEnergyErr = 0f, maxDeltaErr = 0f;

        for (int trial = 0; trial < 100; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);

            ComparisonResult j = JAVA.compareWithPhaseDelta(a, b);
            ComparisonResult n = NATIVE.compareWithPhaseDelta(a, b);

            float eErr = Math.abs(j.energy() - n.energy());
            float dErr = (float) Math.abs(j.phaseDelta() - n.phaseDelta());
            maxEnergyErr = Math.max(maxEnergyErr, eErr);
            maxDeltaErr = Math.max(maxDeltaErr, dErr);

            assertTrue(eErr <= EPSILON,
                    "Default phaseDelta energy trial=" + trial + " java=" + j.energy() + " native=" + n.energy() + " err=" + eErr);
            assertTrue(dErr <= 0.01f,
                    "Default phaseDelta delta trial=" + trial + " java=" + j.phaseDelta() + " native=" + n.phaseDelta() + " err=" + dErr);
        }
        System.out.printf("  D=%d default delta maxEnergyErr=%.2e maxDeltaErr=%.2e%n", dim, maxEnergyErr, maxDeltaErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: compareWithPhaseDelta — weighted path")
    void phaseDeltaWeighted(int dim) {
        requireNative();
        Random rng = new Random(SEED);
        float maxEnergyErr = 0f, maxDeltaErr = 0f;

        for (int trial = 0; trial < 100; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);
            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(randomWeights(rng, dim)));

            ComparisonResult j = JAVA.compareWithPhaseDelta(a, b, opts);
            ComparisonResult n = NATIVE.compareWithPhaseDelta(a, b, opts);

            float eErr = Math.abs(j.energy() - n.energy());
            float dErr = (float) Math.abs(j.phaseDelta() - n.phaseDelta());
            maxEnergyErr = Math.max(maxEnergyErr, eErr);
            maxDeltaErr = Math.max(maxDeltaErr, dErr);

            assertTrue(eErr <= EPSILON,
                    "Weighted phaseDelta energy trial=" + trial + " java=" + j.energy() + " native=" + n.energy() + " err=" + eErr);
            assertTrue(dErr <= 0.01f,
                    "Weighted phaseDelta delta trial=" + trial + " java=" + j.phaseDelta() + " native=" + n.phaseDelta() + " err=" + dErr);
        }
        System.out.printf("  D=%d weighted delta maxEnergyErr=%.2e maxDeltaErr=%.2e%n", dim, maxEnergyErr, maxDeltaErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: compareWithPhaseDelta — phase-free path")
    void phaseDeltaPhaseFree(int dim) {
        requireNative();
        Random rng = new Random(SEED);
        float maxEnergyErr = 0f;

        for (int trial = 0; trial < 100; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);
            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(allZero(dim)));

            ComparisonResult j = JAVA.compareWithPhaseDelta(a, b, opts);
            ComparisonResult n = NATIVE.compareWithPhaseDelta(a, b, opts);

            float eErr = Math.abs(j.energy() - n.energy());
            maxEnergyErr = Math.max(maxEnergyErr, eErr);

            assertTrue(eErr <= EPSILON,
                    "Phase-free phaseDelta energy trial=" + trial + " java=" + j.energy() + " native=" + n.energy() + " err=" + eErr);
            assertEquals(0.0, n.phaseDelta(), 1e-10, "phase-free phaseDelta must be 0");
        }
        System.out.printf("  D=%d phase-free delta maxEnergyErr=%.2e%n", dim, maxEnergyErr);
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: self-compare = 1.0 on both kernels, all paths")
    void selfCompare(int dim) {
        requireNative();
        Random rng = new Random(SEED);

        for (int trial = 0; trial < 20; trial++) {
            WavePattern p = randomPattern(rng, dim);

            assertEquals(1.0f, JAVA.compare(p, p), EPSILON, "Java self-compare");
            assertEquals(1.0f, NATIVE.compare(p, p), EPSILON, "Native self-compare");

            CompareOptions opts = CompareOptions.withPhaseWeights(new PhaseWeights(randomWeights(rng, dim)));
            assertEquals(1.0f, JAVA.compare(p, p, opts), EPSILON, "Java weighted self-compare");
            assertEquals(1.0f, NATIVE.compare(p, p, opts), EPSILON, "Native weighted self-compare");

            CompareOptions optsFree = CompareOptions.withPhaseWeights(new PhaseWeights(allZero(dim)));
            assertEquals(1.0f, JAVA.compare(p, p, optsFree), EPSILON, "Java phase-free self-compare");
            assertEquals(1.0f, NATIVE.compare(p, p, optsFree), EPSILON, "Native phase-free self-compare");
        }
    }

    @ParameterizedTest(name = "D={0}")
    @ValueSource(ints = {1, 7, 17, 127, 513})
    @DisplayName("Parity: all-one weights = default on both kernels")
    void allOneEqualsDefault(int dim) {
        requireNative();
        Random rng = new Random(SEED);
        CompareOptions optsOne = CompareOptions.withPhaseWeights(new PhaseWeights(allOne(dim)));

        for (int trial = 0; trial < 50; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);

            float jDef = JAVA.compare(a, b);
            float jOne = JAVA.compare(a, b, optsOne);
            float nDef = NATIVE.compare(a, b);
            float nOne = NATIVE.compare(a, b, optsOne);

            assertEquals(jDef, jOne, EPSILON, "Java: all-one must equal default");
            assertEquals(nDef, nOne, EPSILON, "Native: all-one must equal default");
        }
    }
}
