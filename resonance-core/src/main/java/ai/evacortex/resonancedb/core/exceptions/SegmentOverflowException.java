/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.exceptions;


public class SegmentOverflowException extends RuntimeException {

    public SegmentOverflowException(String message) {
        super(message);
    }

    public SegmentOverflowException(String message, Throwable cause) {
        super(message, cause);
    }
}