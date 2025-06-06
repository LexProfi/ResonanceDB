/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

/**
 * Configuration options for fine-tuning resonance comparison.
 */
public record CompareOptions(
        boolean normalizeAmplitude,     // normalize amplitude to [0...1] per pattern
        boolean ignorePhase,            // ignore phase completely (project to magnitude only)
        boolean allowGlobalPhaseShift   // align by global φ (not yet implemented)
) {
    public static CompareOptions defaultOptions() {
        return new CompareOptions(false, false, false);
    }
}