/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

abstract class ResonanceKernelContractTest {

    protected abstract ResonanceKernel kernel();

    protected static WavePattern constPattern(double amp, double phase, int len) {
        double[] a = new double[len];
        double[] p = new double[len];
        Arrays.fill(a, amp);
        Arrays.fill(p, phase);
        return new WavePattern(a, p);
    }

    protected static WavePattern rampAmpPattern(double start, double step, double phase, int len) {
        double[] a = new double[len];
        double[] p = new double[len];
        for (int i = 0; i < len; i++) {
            a[i] = start + i * step;
            p[i] = phase;
        }
        return new WavePattern(a, p);
    }

    protected static WavePattern randomPattern(int len, long seed) {
        Random r = new Random(seed);
        double[] a = new double[len];
        double[] p = new double[len];
        for (int i = 0; i < len; i++) {
            a[i] = r.nextDouble();
            p[i] = r.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(a, p);
    }

    @Test
    void identicalPatterns_yieldOne() {
        WavePattern a = rampAmpPattern(0.1, 0.01, 0.5, 1024);
        float s1 = kernel().compare(a, a);
        float s2 = kernel().compare(a, a, CompareOptions.defaultOptions());
        assertEquals(1.0f, s1, 1e-6, "Identical patterns must score 1.0");
        assertEquals(1.0f, s2, 1e-6, "Identical patterns must score 1.0 with default options");
    }

    @Test
    void oppositePhase_sameAmplitude_yieldZero() {
        WavePattern a = constPattern(1.0, 0.0, 1024);
        WavePattern b = constPattern(1.0, Math.PI, 1024);
        float s = kernel().compare(a, b);
        assertEquals(0.0f, s, 1e-6, "Opposite phase with equal amplitude must score 0.0");
    }

    @Test
    void ignorePhase_turnsOppositeIntoOne() {
        WavePattern a = constPattern(1.0, 0.0, 1024);
        WavePattern b = constPattern(1.0, Math.PI, 1024);
        CompareOptions opts = new CompareOptions(false, true, false, false);
        float s = kernel().compare(a, b, opts);
        assertEquals(1.0f, s, 1e-6, "With ignorePhase=true, opposite phases must behave as identical");
    }

    @Test
    void boundsAreRespected() {
        WavePattern a = randomPattern(1024, 42);
        WavePattern b = randomPattern(1024, 43);
        float s = kernel().compare(a, b);
        assertTrue(s >= 0.0f && s <= 1.0f, "Score must be in [0..1]");
    }

    @Test
    void symmetryHolds() {
        WavePattern a = randomPattern(1024, 1001);
        WavePattern b = randomPattern(1024, 1002);
        float s1 = kernel().compare(a, b);
        float s2 = kernel().compare(b, a);
        assertEquals(s1, s2, 1e-6, "compare(a,b) must equal compare(b,a)");
    }

    @Test
    void determinismHolds() {
        WavePattern a = randomPattern(1024, 7);
        WavePattern b = randomPattern(1024, 8);
        float s1 = kernel().compare(a, b);
        float s2 = kernel().compare(a, b);
        assertEquals(s1, s2, 0.0, "Same input must yield identical result (deterministic)");
    }

    @Test
    void zeroEnergyPairs_yieldZero() {
        WavePattern a = constPattern(0.0, 0.0, 1024);
        WavePattern b = constPattern(0.0, Math.PI, 1024);
        float s = kernel().compare(a, b);
        assertEquals(0.0f, s, 0.0, "If both energies are zero, result must be 0.0");
    }

    @Test
    void compareMany_alignsWithSingleCompare_defaultOptions() {
        WavePattern q = randomPattern(512, 11);
        List<WavePattern> cands = List.of(
                randomPattern(512, 12),
                randomPattern(512, 13),
                randomPattern(512, 14)
        );

        float[] batch = kernel().compareMany(q, cands);
        assertEquals(cands.size(), batch.length);

        for (int i = 0; i < cands.size(); i++) {
            float s = kernel().compare(q, cands.get(i));
            assertEquals(s, batch[i], 1e-6, "compareMany must equal per-item compare() with default options");
        }
    }

    @Test
    void compareMany_withIgnorePhase_matchesPerItem() {
        WavePattern q = randomPattern(256, 21);
        List<WavePattern> cands = List.of(
                constPattern(1.0, 0.0, 256),
                constPattern(1.0, Math.PI, 256),
                rampAmpPattern(0.2, 0.005, Math.PI / 3, 256)
        );
        CompareOptions opts = new CompareOptions(false, true, false, false);

        float[] batch = kernel().compareMany(q, cands, opts);
        assertEquals(cands.size(), batch.length);

        for (int i = 0; i < cands.size(); i++) {
            float s = kernel().compare(q, cands.get(i), opts);
            assertEquals(s, batch[i], 1e-6, "compareMany(opts) must equal per-item compare(opts)");
        }
    }

    @Test
    void phaseDelta_identical_isZero_andEnergyIsOneRaw() {
        WavePattern a = constPattern(0.8, 0.75, 1024);
        ComparisonResult r = kernel().compareWithPhaseDelta(a, a);
        assertEquals(1.0f, r.energy(), 1e-6, "Raw normalized energy should be 1.0 for identical");
        assertEquals(0.0, Math.abs(r.phaseDelta()), 1e-9, "Phase delta should be ~0 for identical");
        assertTrue(r.phaseDelta() >= -Math.PI && r.phaseDelta() <= Math.PI, "Δφ must be in [-π, π]");
    }

    @Test
    void phaseDelta_opposite_isPi_orMinusPi_andEnergyZeroRaw() {
        WavePattern a = constPattern(1.0, 0.0, 2048);
        WavePattern b = constPattern(1.0, Math.PI, 2048);
        ComparisonResult r = kernel().compareWithPhaseDelta(a, b);
        assertEquals(0.0f, r.energy(), 1e-6, "Raw normalized energy should be 0.0 for opposite phase");
        assertTrue(Math.abs(Math.abs(r.phaseDelta()) - Math.PI) < 1e-6, "Δφ magnitude should be ~π");
    }

    @Test
    void nullArguments_throwNpe() {
        WavePattern a = constPattern(1.0, 0.0, 8);
        assertThrows(NullPointerException.class, () -> kernel().compare(null, a));
        assertThrows(NullPointerException.class, () -> kernel().compare(a, null));
        assertThrows(NullPointerException.class, () -> kernel().compare(null, null));

        assertThrows(NullPointerException.class, () -> kernel().compare(a, a, null));
        assertThrows(NullPointerException.class, () -> kernel().compareMany(null, List.of(a)));
        assertThrows(NullPointerException.class, () -> kernel().compareMany(a, null));
        assertThrows(NullPointerException.class, () -> kernel().compareMany(null, null));
        assertThrows(NullPointerException.class, () -> kernel().compareMany(a, List.of(a), null));
        assertThrows(NullPointerException.class, () -> kernel().compareWithPhaseDelta(null, a));
        assertThrows(NullPointerException.class, () -> kernel().compareWithPhaseDelta(a, null));
    }

    @Test
    void lengthMismatch_throwsIae() {
        WavePattern a = constPattern(1.0, 0.0, 7);
        WavePattern b = constPattern(1.0, 0.0, 9);
        assertThrows(IllegalArgumentException.class, () -> kernel().compare(a, b));
        assertThrows(IllegalArgumentException.class, () -> kernel().compare(a, b, CompareOptions.defaultOptions()));

        List<WavePattern> cands = new ArrayList<>();
        cands.add(constPattern(1.0, 0.0, 7));
        cands.add(constPattern(1.0, 0.0, 9));
        assertThrows(IllegalArgumentException.class, () -> kernel().compareMany(a, cands));
        assertThrows(IllegalArgumentException.class, () -> kernel().compareMany(a, cands, CompareOptions.defaultOptions()));

        assertThrows(IllegalArgumentException.class, () -> kernel().compareWithPhaseDelta(a, b));
    }

    @Test
    void balancedAmplitudes_phaseAligned_scoreIsHigh() {
        WavePattern a = constPattern(0.5, 0.2, 1024);
        WavePattern b = constPattern(0.5, 0.2, 1024);
        float s = kernel().compare(a, b);
        assertEquals(1.0f, s, 1e-6);
    }

    @Test
    void unbalancedAmplitudes_phaseAligned_scoreLessThanOne() {
        WavePattern a = constPattern(1.0, 0.0, 1024);
        WavePattern b = constPattern(0.01, 0.0, 1024);
        float s = kernel().compare(a, b);
        assertTrue(s > 0.0f && s < 1.0f, "With large amplitude imbalance result must be (0,1)");
    }

    @Test
    void fuzz_symmetryAndBounds() {
        for (int t = 0; t < 20; t++) {
            WavePattern a = randomPattern(257, 1000 + t);
            WavePattern b = randomPattern(257, 2000 + t);
            float s1 = kernel().compare(a, b);
            float s2 = kernel().compare(b, a);
            assertEquals(s1, s2, 1e-6, "Symmetry failed at iter " + t);
            assertTrue(s1 >= 0.0f && s1 <= 1.0f, "Bounds failed at iter " + t);
        }
    }
}

@DisplayName("ResonanceKernel contract tests (JavaKernel)")
class JavaKernelContractTest extends ResonanceKernelContractTest {
    @Override
    protected ResonanceKernel kernel() {
        return new JavaKernel();
    }
}
