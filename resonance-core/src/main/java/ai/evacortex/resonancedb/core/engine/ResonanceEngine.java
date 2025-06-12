/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import ai.evacortex.resonancedb.core.storage.WavePattern;

public class ResonanceEngine {

    private static ResonanceKernel backend = new JavaKernel();

    public static void setBackend(ResonanceKernel kernel) {
        backend = kernel;
    }

    public static float compare(WavePattern a, WavePattern b) {
        return backend.compare(a, b);
    }

    public static ComparisonResult compareWithPhaseDelta(WavePattern a, WavePattern b) {
        return backend.compareWithPhaseDelta(a, b);
    }
}