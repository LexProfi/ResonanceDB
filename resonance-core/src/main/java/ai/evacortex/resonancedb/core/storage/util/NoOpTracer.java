/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.util;

import ai.evacortex.resonancedb.core.storage.ResonanceTracer;
import ai.evacortex.resonancedb.core.storage.WavePattern;

public class NoOpTracer implements ResonanceTracer {
    @Override
    public void trace(String id, WavePattern query, WavePattern matched, float score) {
        // no-op
    }
}