/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.handlers;

import ai.evacortex.resonancedb.rest.dto.HealthResponse;
import com.sun.net.httpserver.HttpExchange;

import java.time.Instant;

public final class HealthHandlers {

    public HealthHandlers() {}

    public HealthResponse health(HttpExchange ex) {
        return new HealthResponse("ok", Instant.now().toString());
    }
}