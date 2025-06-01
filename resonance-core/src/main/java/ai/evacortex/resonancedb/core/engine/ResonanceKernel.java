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

public interface ResonanceKernel {
    /**
     * Computes the normalized resonance energy between two wave patterns.
     * Formula: R(ψ₁, ψ₂) = ∫|ψ₁ + ψ₂|² / (∫|ψ₁|² + ∫|ψ₂|²)
     *
     * @param a WavePattern A
     * @param b WavePattern B
     * @return Resonance score ∈ [0.0 ... 1.0]
     */
    float compare(WavePattern a, WavePattern b);
}