/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.exceptions;

public class InvalidWavePatternException extends RuntimeException {
    public InvalidWavePatternException(String message) {
        super("Invalid WavePattern: " + message);
    }

    public InvalidWavePatternException(String message, Throwable cause) {
        super("Invalid WavePattern: " + message, cause);
    }
}