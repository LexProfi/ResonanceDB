/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.error;

import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.rest.dto.ErrorResponse;
import com.fasterxml.jackson.core.JsonProcessingException;

public final class ErrorMapper {

    public RestError map(Throwable t) {

        // 400: explicit BadRequest
        if (t instanceof BadRequestException br) {
            return new RestError(
                    400,
                    new ErrorResponse("bad_request", safeMsg(br))
            );
        }

        // 400: JSON parse errors
        if (t instanceof JsonProcessingException jpe) {
            return new RestError(
                    400,
                    new ErrorResponse(
                            "bad_json",
                            "Invalid JSON: " + jpe.getOriginalMessage()
                    )
            );
        }

        // 400: invalid pattern (domain validation)
        if (t instanceof InvalidWavePatternException iwpe) {
            return new RestError(
                    400,
                    new ErrorResponse("invalid_pattern", safeMsg(iwpe))
            );
        }

        // 409: duplicate
        if (t instanceof DuplicatePatternException dpe) {
            return new RestError(
                    409,
                    new ErrorResponse("duplicate", safeMsg(dpe))
            );
        }

        // 404: not found
        if (t instanceof PatternNotFoundException pnfe) {
            return new RestError(
                    404,
                    new ErrorResponse("not_found", safeMsg(pnfe))
            );
        }

        // Fallback: 500
        return new RestError(
                500,
                new ErrorResponse("internal_error", safeMsg(t))
        );
    }

    /**
     * Preserves original safeMsg() semantics:
     *  - If message is null/blank → use simple class name.
     *  - Otherwise use original message.
     */
    private static String safeMsg(Throwable t) {
        String m = t.getMessage();
        if (m == null || m.isBlank()) {
            return t.getClass().getSimpleName();
        }
        return m;
    }
}