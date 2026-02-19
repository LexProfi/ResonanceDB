/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.io;

import ai.evacortex.resonancedb.rest.ResonanceRestConfig;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.Objects;

public final class CorsSupport {

    private final ResonanceRestConfig cfg;

    public CorsSupport(ResonanceRestConfig cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    public void apply(HttpExchange ex) {
        ex.getResponseHeaders()
                .set("Access-Control-Allow-Origin", cfg.corsAllowOrigin());
    }

    public void preflight(HttpExchange ex) throws IOException {
        Headers h = ex.getResponseHeaders();
        h.set("Access-Control-Allow-Origin", cfg.corsAllowOrigin());
        h.set("Access-Control-Allow-Methods", cfg.corsAllowMethods());
        h.set("Access-Control-Allow-Headers", cfg.corsAllowHeaders());
        ex.sendResponseHeaders(204, -1);
    }
}