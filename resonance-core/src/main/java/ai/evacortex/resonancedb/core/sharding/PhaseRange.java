/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.sharding;

public record PhaseRange(double start, double end) {

    public PhaseRange {
        if (start < 0.0 || end > Math.PI || start >= end) {
            throw new IllegalArgumentException("Invalid phase range: [" + start + " .. " + end + "]");
        }
    }

    public boolean contains(double phase) {
        return phase >= start && phase < end;
    }

    public boolean overlaps(PhaseRange other) {
        return this.start < other.end && other.start < this.end;
    }

    @Override
    public String toString() {
        return "[" + start + " .. " + end + "]";
    }
}