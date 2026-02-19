/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.http;

import com.sun.net.httpserver.HttpServer;

import java.util.Objects;

public final class RestRouter {

    private final HttpServer server;
    private final RestPipeline pipeline;

    public RestRouter(HttpServer server, RestPipeline pipeline) {
        this.server = Objects.requireNonNull(server, "server");
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    }

    public void get(String path, ExchangeHandlers.ExchangeHandler handler) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(handler, "handler");
        server.createContext(path, ex -> pipeline.handleGet(ex, handler));
    }

    public void post(String path, ExchangeHandlers.ExchangeHandler handler) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(handler, "handler");
        server.createContext(path, ex -> pipeline.handlePost(ex, handler));
    }

    public <Req, Resp> void postJson(
            String path,
            Class<Req> reqType,
            ExchangeHandlers.ExchangeJsonHandler<Req, Resp> handler
    ) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(reqType, "reqType");
        Objects.requireNonNull(handler, "handler");
        server.createContext(path, ex -> pipeline.handlePostJson(ex, reqType, handler));
    }
}