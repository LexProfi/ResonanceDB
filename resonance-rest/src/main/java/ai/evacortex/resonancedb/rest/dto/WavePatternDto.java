/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.dto;

public final class WavePatternDto {

    public double[] amplitude;
    public double[] phase;

    public WavePatternDto() {
        // Required by Jackson
    }

    public WavePatternDto(double[] amplitude, double[] phase) {
        this.amplitude = amplitude;
        this.phase = phase;
    }
}