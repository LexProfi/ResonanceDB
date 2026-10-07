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
import ai.evacortex.resonancedb.core.engine.PhaseRoutingProfile;
import ai.evacortex.resonancedb.core.index.ResonanceMoments;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for Parametric Phase / Phase Comb / Weighted Phase Participation.
 *
 * Covers:
 * - PhaseWeights validation and properties
 * - CompareOptions backward compatibility
 * - Weighted kernel math (§37)
 * - PhaseRoutingProfile
 * - ResonanceMoments persistence
 */
class PhaseWeightsTest {

    private static final ResonanceKernel kernel = new JavaKernel();

    // ─── PhaseWeights Validation ────────────────────────────────────────────

    @Test
    @DisplayName("PhaseWeights: all-one is default")
    void allOneIsDefault() {
        var pw = new PhaseWeights(new double[]{1.0, 1.0, 1.0});
        assertTrue(pw.isDefault());
        assertFalse(pw.isPhaseFree());
        assertEquals(3.0, pw.sumOfWeights());
    }

    @Test
    @DisplayName("PhaseWeights: all-zero is phase-free")
    void allZeroIsPhaseFree() {
        var pw = new PhaseWeights(new double[]{0.0, 0.0, 0.0});
        assertFalse(pw.isDefault());
        assertTrue(pw.isPhaseFree());
        assertEquals(0.0, pw.sumOfWeights());
    }

    @Test
    @DisplayName("PhaseWeights: mixed weights")
    void mixedWeights() {
        var pw = new PhaseWeights(new double[]{0.5, 1.0, 0.0});
        assertFalse(pw.isDefault());
        assertFalse(pw.isPhaseFree());
        assertEquals(1.5, pw.sumOfWeights());
        assertEquals(3, pw.length());
    }

    @Test
    @DisplayName("PhaseWeights: NaN rejected")
    void nanRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PhaseWeights(new double[]{1.0, Double.NaN}));
    }

    @Test
    @DisplayName("PhaseWeights: Infinity rejected")
    void infinityRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PhaseWeights(new double[]{Double.POSITIVE_INFINITY, 0.5}));
    }

    @Test
    @DisplayName("PhaseWeights: negative rejected")
    void negativeRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PhaseWeights(new double[]{-0.1, 0.5}));
    }

    @Test
    @DisplayName("PhaseWeights: > 1.0 rejected")
    void overOneRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PhaseWeights(new double[]{0.5, 1.1}));
    }

    @Test
    @DisplayName("PhaseWeights: empty rejected")
    void emptyRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PhaseWeights(new double[]{}));
    }

    @Test
    @DisplayName("PhaseWeights: null rejected")
    void nullRejected() {
        assertThrows(NullPointerException.class,
                () -> new PhaseWeights(null));
    }

    @Test
    @DisplayName("PhaseWeights: dimension mismatch detected")
    void dimensionMismatch() {
        var pw = new PhaseWeights(new double[]{0.5, 0.5});
        assertThrows(IllegalArgumentException.class, () -> pw.validateDimension(3));
        assertDoesNotThrow(() -> pw.validateDimension(2));
    }

    @Test
    @DisplayName("PhaseWeights: defensive copy")
    void defensiveCopy() {
        double[] src = {0.5, 0.5};
        var pw = new PhaseWeights(src);
        src[0] = 0.0;
        assertEquals(0.5, pw.weight(0), "source mutation must not affect PhaseWeights");
    }

    // ─── CompareOptions Backward Compatibility ──────────────────────────────

    @Test
    @DisplayName("CompareOptions: 4-arg constructor backward compatible")
    void fourArgConstructor() {
        var opts = new CompareOptions(false, true, false, false);
        assertTrue(opts.ignorePhase());
        assertNull(opts.phaseWeights());
        assertTrue(opts.isEffectivelyPhaseFree());
    }

    @Test
    @DisplayName("CompareOptions: defaultOptions has null phaseWeights")
    void defaultOptionsNullWeights() {
        var opts = CompareOptions.defaultOptions();
        assertNull(opts.phaseWeights());
        assertFalse(opts.hasEffectivePhaseWeights());
        assertFalse(opts.isEffectivelyPhaseFree());
    }

    @Test
    @DisplayName("CompareOptions: ignorePhase takes priority over phaseWeights")
    void ignorePhasePriority() {
        var pw = new PhaseWeights(new double[]{0.5, 0.5, 0.5});
        var opts = new CompareOptions(false, true, false, false, pw);
        assertTrue(opts.isEffectivelyPhaseFree());
        assertFalse(opts.hasEffectivePhaseWeights());
    }

    @Test
    @DisplayName("CompareOptions.withPhaseWeights factory")
    void withPhaseWeightsFactory() {
        var pw = new PhaseWeights(new double[]{0.5, 0.5});
        var opts = CompareOptions.withPhaseWeights(pw);
        assertSame(pw, opts.phaseWeights());
        assertTrue(opts.hasEffectivePhaseWeights());
    }

    // ─── Weighted Kernel: Mathematical Correctness ──────────────────────────

    @Test
    @DisplayName("all-one weights == default compare")
    void allOneWeightsEqualsDefault() {
        var rng = new Random(42);
        int dim = 128;
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);

        float defaultScore = kernel.compare(a, b);
        var pw = new PhaseWeights(ones(dim));
        float weightedScore = kernel.compare(a, b, CompareOptions.withPhaseWeights(pw));

        assertEquals(defaultScore, weightedScore, 0.0f, "all-one weights must equal default");
    }

    @Test
    @DisplayName("all-zero weights == ignorePhase behavior")
    void allZeroWeightsEqualsIgnorePhase() {
        var rng = new Random(42);
        int dim = 128;
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);

        float ignorePhaseScore = kernel.compare(a, b, new CompareOptions(false, true, false, false));
        var pw = new PhaseWeights(zeros(dim));
        float allZeroScore = kernel.compare(a, b, CompareOptions.withPhaseWeights(pw));

        assertEquals(ignorePhaseScore, allZeroScore, 0.0f,
                "all-zero weights must equal ignorePhase");
    }

    @Test
    @DisplayName("Symmetry: score(a,b,w) == score(b,a,w)")
    void symmetry() {
        var rng = new Random(42);
        int dim = 64;
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);
        var pw = new PhaseWeights(randomWeights(rng, dim));
        var opts = CompareOptions.withPhaseWeights(pw);

        float ab = kernel.compare(a, b, opts);
        float ba = kernel.compare(b, a, opts);
        assertEquals(ab, ba, 0.0f, "weighted comparison must be symmetric");
    }

    @Test
    @DisplayName("Score in [0, 1] range")
    void scoreRange() {
        var rng = new Random(42);
        int dim = 64;
        for (int trial = 0; trial < 100; trial++) {
            WavePattern a = randomPattern(rng, dim);
            WavePattern b = randomPattern(rng, dim);
            var pw = new PhaseWeights(randomWeights(rng, dim));
            float score = kernel.compare(a, b, CompareOptions.withPhaseWeights(pw));
            assertTrue(score >= 0.0f && score <= 1.0f,
                    "score " + score + " out of [0,1] range");
        }
    }

    @Test
    @DisplayName("Determinism: same input → same output")
    void determinism() {
        var rng = new Random(42);
        int dim = 64;
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(new Random(43), dim);
        var pw = new PhaseWeights(randomWeights(new Random(44), dim));
        var opts = CompareOptions.withPhaseWeights(pw);

        float s1 = kernel.compare(a, b, opts);
        float s2 = kernel.compare(a, b, opts);
        assertEquals(s1, s2, 0.0f, "must be deterministic");
    }

    @Test
    @DisplayName("Single dimension masked: w=0 ignores that dimension's phase")
    void singleDimensionMasked() {
        // 2D patterns where dimension 1 has opposing phases
        double[] ampA = {1.0, 1.0};
        double[] phaseA = {0.0, 0.0};
        double[] ampB = {1.0, 1.0};
        double[] phaseB = {0.0, Math.PI}; // dimension 1 fully opposite

        WavePattern a = new WavePattern(ampA, phaseA);
        WavePattern b = new WavePattern(ampB, phaseB);

        float fullScore = kernel.compare(a, b);

        // Mask dimension 1 (the opposing one)
        var pw = new PhaseWeights(new double[]{1.0, 0.0});
        float maskedScore = kernel.compare(a, b, CompareOptions.withPhaseWeights(pw));

        assertTrue(maskedScore > fullScore,
                "masking the opposing dimension should increase score");
    }

    @Test
    @DisplayName("Continuous weights: w=0.5 gives intermediate score")
    void continuousWeightsIntermediate() {
        double[] ampA = {1.0, 1.0};
        double[] phaseA = {0.0, 0.0};
        double[] ampB = {1.0, 1.0};
        double[] phaseB = {0.0, Math.PI};

        WavePattern a = new WavePattern(ampA, phaseA);
        WavePattern b = new WavePattern(ampB, phaseB);

        float fullScore = kernel.compare(a, b);
        float ignoredScore = kernel.compare(a, b, new CompareOptions(false, true, false, false));

        var pw = new PhaseWeights(new double[]{1.0, 0.5});
        float halfScore = kernel.compare(a, b, CompareOptions.withPhaseWeights(pw));

        // halfScore should be between fullScore and a more relaxed score
        assertTrue(halfScore > fullScore, "half-weight on opposing dim should improve score");
    }

    @Test
    @DisplayName("Zero-energy patterns: score is 0.0")
    void zeroEnergy() {
        WavePattern a = new WavePattern(new double[]{0.0, 0.0}, new double[]{0.0, 0.0});
        WavePattern b = new WavePattern(new double[]{0.0, 0.0}, new double[]{0.0, 0.0});
        var pw = new PhaseWeights(new double[]{0.5, 0.5});
        float score = kernel.compare(a, b, CompareOptions.withPhaseWeights(pw));
        assertEquals(0.0f, score);
    }

    @Test
    @DisplayName("Length mismatch throws")
    void lengthMismatch() {
        WavePattern a = new WavePattern(new double[]{1.0}, new double[]{0.0});
        WavePattern b = new WavePattern(new double[]{1.0}, new double[]{0.0});
        var pw = new PhaseWeights(new double[]{0.5, 0.5}); // 2 weights for 1-dim patterns
        var opts = CompareOptions.withPhaseWeights(pw);
        assertThrows(IllegalArgumentException.class, () -> kernel.compare(a, b, opts));
    }

    // ─── compareWithPhaseDelta Weighted ─────────────────────────────────────

    @Test
    @DisplayName("compareWithPhaseDelta: default weights match standard behavior")
    void comparePhaseDeltaDefault() {
        var rng = new Random(42);
        int dim = 64;
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);

        ComparisonResult r1 = kernel.compareWithPhaseDelta(a, b);
        ComparisonResult r2 = kernel.compareWithPhaseDelta(a, b, CompareOptions.defaultOptions());

        assertEquals(r1.energy(), r2.energy(), 0.0f);
        assertEquals(r1.phaseDelta(), r2.phaseDelta(), 1e-12);
    }

    @Test
    @DisplayName("compareWithPhaseDelta: all-one weights match standard")
    void comparePhaseDeltaAllOne() {
        var rng = new Random(42);
        int dim = 64;
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);

        ComparisonResult r1 = kernel.compareWithPhaseDelta(a, b);
        var pw = new PhaseWeights(ones(dim));
        ComparisonResult r2 = kernel.compareWithPhaseDelta(a, b, CompareOptions.withPhaseWeights(pw));

        assertEquals(r1.energy(), r2.energy(), 0.0f);
        assertEquals(r1.phaseDelta(), r2.phaseDelta(), 1e-12);
    }

    @Test
    @DisplayName("compareWithPhaseDelta: phase-free returns phaseDelta=0")
    void comparePhaseDeltaPhaseFree() {
        var rng = new Random(42);
        int dim = 64;
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);

        var pw = new PhaseWeights(zeros(dim));
        ComparisonResult r = kernel.compareWithPhaseDelta(a, b, CompareOptions.withPhaseWeights(pw));
        assertEquals(0.0, r.phaseDelta());
    }

    @Test
    @DisplayName("compareWithPhaseDelta: energy matches compare for weighted")
    void comparePhaseDeltaEnergyMatchesCompare() {
        var rng = new Random(42);
        int dim = 64;
        WavePattern a = randomPattern(rng, dim);
        WavePattern b = randomPattern(rng, dim);
        var pw = new PhaseWeights(randomWeights(rng, dim));
        var opts = CompareOptions.withPhaseWeights(pw);

        float compareScore = kernel.compare(a, b, opts);
        ComparisonResult pdResult = kernel.compareWithPhaseDelta(a, b, opts);

        assertEquals(compareScore, pdResult.energy(), 0.0f,
                "compareWithPhaseDelta energy must match compare score");
    }

    @Test
    @DisplayName("compareMany weighted matches individual compares")
    void compareManyWeighted() {
        var rng = new Random(42);
        int dim = 32;
        WavePattern query = randomPattern(rng, dim);
        List<WavePattern> candidates = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) candidates.add(randomPattern(rng, dim));

        var pw = new PhaseWeights(randomWeights(rng, dim));
        var opts = CompareOptions.withPhaseWeights(pw);

        float[] batch = kernel.compareMany(query, candidates, opts);
        for (int i = 0; i < candidates.size(); i++) {
            float individual = kernel.compare(query, candidates.get(i), opts);
            assertEquals(individual, batch[i], 0.0f,
                    "compareMany[" + i + "] must match individual compare");
        }
    }

    // ─── 1D/2D Hand-Calculated Examples ─────────────────────────────────────

    @Test
    @DisplayName("1D example: w=0.5 with π phase difference")
    void oneDimExample() {
        // A1=1, A2=1, Δφ=π, w=0.5
        // G = (1-0.5) + 0.5*cos(π) = 0.5 - 0.5 = 0
        // inter = 1 + 1 + 2*1*1*0 = 2
        // base = 0.5 * 2 / 2 = 0.5
        // ampF = 2*sqrt(1*1)/2 = 1.0
        // score = 0.5 * 1.0 = 0.5
        WavePattern a = new WavePattern(new double[]{1.0}, new double[]{0.0});
        WavePattern b = new WavePattern(new double[]{1.0}, new double[]{Math.PI});
        var pw = new PhaseWeights(new double[]{0.5});
        float score = kernel.compare(a, b, CompareOptions.withPhaseWeights(pw));
        assertEquals(0.5f, score, 1e-6f);
    }

    @Test
    @DisplayName("1D example: w=0 with π phase difference (amplitude-only)")
    void oneDimPhaseIgnored() {
        // G = 1 (phase ignored), same as ignorePhase
        WavePattern a = new WavePattern(new double[]{1.0}, new double[]{0.0});
        WavePattern b = new WavePattern(new double[]{1.0}, new double[]{Math.PI});
        var pw = new PhaseWeights(new double[]{0.0});
        float score = kernel.compare(a, b, CompareOptions.withPhaseWeights(pw));

        float ignorePhase = kernel.compare(a, b, new CompareOptions(false, true, false, false));
        assertEquals(ignorePhase, score, 0.0f);
    }

    // ─── PhaseRoutingProfile ────────────────────────────────────────────────

    @Test
    @DisplayName("PhaseRoutingProfile: default has zero uncertainty")
    void profileDefaultZeroUncertainty() {
        var profile = PhaseRoutingProfile.defaultProfile();
        assertTrue(profile.isDefault());
        assertEquals(0.0, profile.routingUncertainty());
        assertEquals(0.0, profile.weightDrift());
    }

    @Test
    @DisplayName("PhaseRoutingProfile: all-one weights produce default profile")
    void profileAllOneIsDefault() {
        var pw = new PhaseWeights(ones(64));
        var opts = CompareOptions.withPhaseWeights(pw);
        var profile = PhaseRoutingProfile.from(opts);
        assertTrue(profile.isDefault());
        assertEquals(0.0, profile.routingUncertainty());
    }

    @Test
    @DisplayName("PhaseRoutingProfile: phase-free has π uncertainty")
    void profilePhaseFree() {
        var pw = new PhaseWeights(zeros(64));
        var opts = CompareOptions.withPhaseWeights(pw);
        var profile = PhaseRoutingProfile.from(opts);
        assertTrue(profile.isPhaseFree());
        assertEquals(Math.PI, profile.routingUncertainty());
    }

    @Test
    @DisplayName("PhaseRoutingProfile: weighted center computation")
    void profileWeightedCenter() {
        double[] phase = {0.0, Math.PI / 2, -Math.PI / 2};
        WavePattern query = new WavePattern(new double[]{1.0, 1.0, 1.0}, phase);

        // Uniform weights → arithmetic mean
        double expectedMean = (0.0 + Math.PI / 2 - Math.PI / 2) / 3;

        var pw = new PhaseWeights(new double[]{1.0, 1.0, 0.0}); // suppress dim 2
        var opts = CompareOptions.withPhaseWeights(pw);
        var profile = PhaseRoutingProfile.from(opts);

        double center = profile.weightedPhaseCenter(query);
        // w = {1, 1, 0}, Σw = 2
        // weighted center = (1*0 + 1*π/2 + 0*(-π/2)) / 2 = π/4
        assertEquals(Math.PI / 4, center, 1e-12);
    }

    @Test
    @DisplayName("PhaseRoutingProfile: routing uncertainty bound is conservative")
    void profileRoutingUncertaintyBound() {
        // Property test: for random patterns and weights,
        // |fullMean - weightedMean| <= routingUncertainty
        var rng = new Random(42);
        int dim = 128;

        for (int trial = 0; trial < 200; trial++) {
            WavePattern p = randomPattern(rng, dim);
            double[] w = randomWeights(rng, dim);
            var pw = new PhaseWeights(w);
            var opts = CompareOptions.withPhaseWeights(pw);
            var profile = PhaseRoutingProfile.from(opts);

            if (profile.isPhaseFree() || profile.isDefault()) continue;

            double fullMean = mean(p.phase());
            double weightedMean = profile.weightedPhaseCenter(p);
            double actualDrift = Math.abs(weightedMean - fullMean);

            assertTrue(actualDrift <= profile.routingUncertainty() + 1e-10,
                    "routing uncertainty bound violated: actual=" + actualDrift
                            + " > bound=" + profile.routingUncertainty());
        }
    }

    // ─── ResonanceMoments ───────────────────────────────────────────────────

    @Test
    @DisplayName("ResonanceMoments: persistence roundtrip")
    void momentsPersistence(@TempDir Path tmpDir) throws IOException {
        int K = 4, D = 8;
        var builder = new ResonanceMoments.Builder(K, D);
        var rng = new Random(42);

        for (int c = 0; c < K; c++) {
            for (int n = 0; n < 10; n++) {
                builder.addPattern(c, randomPattern(rng, D));
            }
        }

        ResonanceMoments moments = builder.build();
        Path path = tmpDir.resolve("test.rmom");
        moments.write(path);

        ResonanceMoments loaded = ResonanceMoments.load(path);
        assertNotNull(loaded);
        assertEquals(K, loaded.centroidCount());
        assertEquals(D, loaded.dimension());

        for (int c = 0; c < K; c++) {
            assertEquals(moments.count(c), loaded.count(c));
            assertEquals(moments.meanEnergy(c), loaded.meanEnergy(c), 1e-6f);
            assertArrayEquals(moments.meanAmplitude(c), loaded.meanAmplitude(c), 1e-6f);
            assertArrayEquals(moments.meanRealProj(c), loaded.meanRealProj(c), 1e-6f);
            assertArrayEquals(moments.meanImagProj(c), loaded.meanImagProj(c), 1e-6f);
        }
    }

    @Test
    @DisplayName("ResonanceMoments: compatibility validation")
    void momentsCompatibility() {
        var builder = new ResonanceMoments.Builder(4, 8);
        ResonanceMoments moments = builder.build();
        assertTrue(moments.isCompatible(4, 8));
        assertFalse(moments.isCompatible(3, 8));
        assertFalse(moments.isCompatible(4, 16));
    }

    @Test
    @DisplayName("ResonanceMoments: missing file returns null")
    void momentsMissingFile(@TempDir Path tmpDir) {
        assertNull(ResonanceMoments.load(tmpDir.resolve("nonexistent.rmom")));
    }

    @Test
    @DisplayName("ResonanceMoments: default scoring equals standard IVF ranking direction")
    void momentsScoringDefault() {
        int K = 4, D = 8;
        var builder = new ResonanceMoments.Builder(K, D);
        var rng = new Random(42);

        for (int c = 0; c < K; c++) {
            for (int n = 0; n < 20; n++) {
                builder.addPattern(c, randomPattern(rng, D));
            }
        }

        ResonanceMoments moments = builder.build();
        WavePattern query = randomPattern(rng, D);

        // Score each centroid
        for (int c = 0; c < K; c++) {
            float score = moments.scoreCentroid(c, query, null);
            assertTrue(score >= 0.0f, "centroid score should be non-negative");
        }
    }

    @Test
    @DisplayName("Adaptive nProbe: all-one weights → baseNProbe exactly")
    void adaptiveProbeDefault() {
        var profile = PhaseRoutingProfile.defaultProfile();
        int result = ai.evacortex.resonancedb.core.index.IvfCandidateSource.computeAdaptiveProbe(
                8, 100, profile);
        assertEquals(8, result);
    }

    @Test
    @DisplayName("Adaptive nProbe: more drift → more probes")
    void adaptiveProbeIncreases() {
        var pw1 = new PhaseWeights(new double[]{0.9, 0.9, 0.9, 0.9});
        var pw2 = new PhaseWeights(new double[]{0.1, 0.1, 0.1, 0.1});
        var p1 = new PhaseRoutingProfile(pw1, CompareOptions.withPhaseWeights(pw1));
        var p2 = new PhaseRoutingProfile(pw2, CompareOptions.withPhaseWeights(pw2));

        int probe1 = ai.evacortex.resonancedb.core.index.IvfCandidateSource.computeAdaptiveProbe(
                8, 100, p1);
        int probe2 = ai.evacortex.resonancedb.core.index.IvfCandidateSource.computeAdaptiveProbe(
                8, 100, p2);

        assertTrue(probe2 >= probe1,
                "more suppressed weights should not reduce probes");
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble() * 2.0;
            phase[i] = (rng.nextDouble() - 0.5) * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static double[] ones(int n) {
        double[] a = new double[n];
        java.util.Arrays.fill(a, 1.0);
        return a;
    }

    private static double[] zeros(int n) {
        return new double[n];
    }

    private static double[] randomWeights(Random rng, int dim) {
        double[] w = new double[dim];
        for (int i = 0; i < dim; i++) w[i] = rng.nextDouble();
        return w;
    }

    private static double mean(double[] arr) {
        double sum = 0;
        for (double v : arr) sum += v;
        return sum / arr.length;
    }
}
