/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */

package ai.evacortex.resonancedb.core.engine;

import ai.evacortex.resonancedb.core.WavePattern;

/**
 * Delegates resonance comparison to backend (Java or Native).
 */
public class ResonanceEngine {

    private static ResonanceKernel backend = new JavaKernel();

    public static void setBackend(ResonanceKernel kernel) {
        backend = kernel;
    }

    public static float compare(WavePattern a, WavePattern b) {
        return backend.compare(a, b);
    }
}