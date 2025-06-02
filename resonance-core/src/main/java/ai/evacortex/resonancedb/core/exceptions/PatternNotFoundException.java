/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.exceptions;

public class PatternNotFoundException extends RuntimeException {
    public PatternNotFoundException(String id) {
        super("Wave pattern with ID '" + id + "' was not found.");
    }

    public PatternNotFoundException(String id, Throwable cause) {
        super("Wave pattern with ID '" + id + "' was not found.", cause);
    }
}