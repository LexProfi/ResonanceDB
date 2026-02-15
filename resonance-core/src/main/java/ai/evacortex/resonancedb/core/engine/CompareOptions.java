/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
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
 *     <li>{@code ignorePhase} — disable phase completely; use only magnitude (A(x))</li>
 *     <li>{@code allowGlobalPhaseShift} — align entire pattern by global φ offset (not yet implemented)</li>
 *     <li>{@code enablePhaseAlignmentBonus} — apply optional bonus when φ_diff &lt; ε (as per patent claim 1(d)(ii))</li>
 * </ul>
 */
public record CompareOptions(
        boolean normalizeAmplitude,
        boolean ignorePhase,
        boolean allowGlobalPhaseShift,
        boolean enablePhaseAlignmentBonus
) {
    public static CompareOptions defaultOptions() {
        return new CompareOptions(false, false, false, false);
    }
}