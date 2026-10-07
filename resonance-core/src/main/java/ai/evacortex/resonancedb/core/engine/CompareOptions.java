/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

/**
 * Configuration options for fine-tuning resonance comparison.
 *
 * <p>These options influence how {@code ResonanceKernel.compare(...)} interprets the input wave patterns.
 * All options are immutable and thread-safe.</p>
 *
 * <ul>
 *     <li>{@code normalizeAmplitude} — normalize each amplitude[] to unit scale before comparison</li>
 *     <li>{@code ignorePhase} — disable phase completely; use only magnitude (A(x)).
 *         When {@code true}, takes priority over {@code phaseWeights} — all phase sensitivity
 *         is suppressed regardless of individual weight values.</li>
 *     <li>{@code allowGlobalPhaseShift} — align entire pattern by global φ offset (not yet implemented)</li>
 *     <li>{@code enablePhaseAlignmentBonus} — apply optional bonus when φ_diff &lt; ε (as per patent claim 1(d)(ii))</li>
 *     <li>{@code phaseWeights} — per-dimension phase participation weights (null = all 1.0)</li>
 * </ul>
 */
public record CompareOptions(
        boolean normalizeAmplitude,
        boolean ignorePhase,
        boolean allowGlobalPhaseShift,
        boolean enablePhaseAlignmentBonus,
        PhaseWeights phaseWeights
) {

    /**
     * Backward-compatible constructor without phase weights.
     */
    public CompareOptions(boolean normalizeAmplitude, boolean ignorePhase,
                          boolean allowGlobalPhaseShift, boolean enablePhaseAlignmentBonus) {
        this(normalizeAmplitude, ignorePhase, allowGlobalPhaseShift, enablePhaseAlignmentBonus, null);
    }

    /**
     * Returns default comparison options: all flags off, no phase weights (full phase sensitivity).
     */
    public static CompareOptions defaultOptions() {
        return new CompareOptions(false, false, false, false, null);
    }

    /**
     * Creates options with the given phase weights and all other flags at default.
     */
    public static CompareOptions withPhaseWeights(PhaseWeights phaseWeights) {
        return new CompareOptions(false, false, false, false, phaseWeights);
    }

    /**
     * Returns {@code true} if phase weights are effectively active (non-null, non-default,
     * and ignorePhase is not set).
     */
    public boolean hasEffectivePhaseWeights() {
        return phaseWeights != null && !phaseWeights.isDefault() && !ignorePhase;
    }

    /**
     * Returns {@code true} if phase should be completely ignored — either via
     * {@code ignorePhase=true} or via all-zero phase weights.
     */
    public boolean isEffectivelyPhaseFree() {
        return ignorePhase || (phaseWeights != null && phaseWeights.isPhaseFree());
    }
}
