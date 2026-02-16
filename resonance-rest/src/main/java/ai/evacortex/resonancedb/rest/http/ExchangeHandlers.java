/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.http;

import com.sun.net.httpserver.HttpExchange;

public final class ExchangeHandlers {

    private ExchangeHandlers() {}

    @FunctionalInterface
    public interface ExchangeHandler {
        void handle(HttpExchange ex) throws Exception;
    }

    @FunctionalInterface
    public interface ExchangeJsonHandler<Req, Resp> {
        Resp handle(HttpExchange ex, Req req) throws Exception;
    }
}