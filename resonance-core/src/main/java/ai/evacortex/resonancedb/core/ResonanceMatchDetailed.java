/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core;

/**
 * Represents a detailed match result including phase alignment and semantic zone.
 */
public record ResonanceMatchDetailed(
        String id,
        float energy,
        WavePattern pattern,
        double phaseDelta,
        ResonanceZone zone,
        double zoneScore
) {}