/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.exceptions;

public class DuplicatePatternException extends RuntimeException {
    public DuplicatePatternException(String id) {
        super("Pattern with id already exists: " + id);
    }
}