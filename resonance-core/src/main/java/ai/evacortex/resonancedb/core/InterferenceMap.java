/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import java.util.List;

/**
 * Represents a semantic interference field in response to a query wave.
 */
public record InterferenceMap(
        WavePattern query,
        List<ResonanceMatchDetailed> matches
) {}