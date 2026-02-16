/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.http;

import ai.evacortex.resonancedb.rest.dto.ErrorResponse;
import ai.evacortex.resonancedb.rest.error.ErrorMapper;
import ai.evacortex.resonancedb.rest.error.RestError;
import ai.evacortex.resonancedb.rest.io.CorsSupport;
import ai.evacortex.resonancedb.rest.io.HttpIO;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.Objects;

public final class RestPipeline {

    private final HttpIO io;
    private final CorsSupport cors;
    private final ErrorMapper errors;

    public RestPipeline(HttpIO io, CorsSupport cors, ErrorMapper errors) {
        this.io = Objects.requireNonNull(io, "io");
        this.cors = Objects.requireNonNull(cors, "cors");
        this.errors = Objects.requireNonNull(errors, "errors");
    }

    public void handleGet(HttpExchange ex, ExchangeHandlers.ExchangeHandler handler) {
        try {
            cors.apply(ex);

            if (isOptions(ex)) {
                cors.preflight(ex);
                return;
            }
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                io.writeJson(ex, 405, new ErrorResponse("method_not_allowed", "Use GET"));
                return;
            }

            handler.handle(ex);

        } catch (Throwable t) {
            writeMappedError(ex, t);
        } finally {
            io.safeClose(ex);
        }
    }

    public void handlePost(HttpExchange ex, ExchangeHandlers.ExchangeHandler handler) {
        try {
            cors.apply(ex);

            if (isOptions(ex)) {
                cors.preflight(ex);
                return;
            }
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                io.writeJson(ex, 405, new ErrorResponse("method_not_allowed", "Use POST"));
                return;
            }

            handler.handle(ex);

        } catch (Throwable t) {
            writeMappedError(ex, t);
        } finally {
            io.safeClose(ex);
        }
    }

    public <Req, Resp> void handlePostJson(
            HttpExchange ex,
            Class<Req> reqType,
            ExchangeHandlers.ExchangeJsonHandler<Req, Resp> handler
    ) {
        try {
            cors.apply(ex);

            if (isOptions(ex)) {
                cors.preflight(ex);
                return;
            }
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                io.writeJson(ex, 405, new ErrorResponse("method_not_allowed", "Use POST"));
                return;
            }

            Req req = io.readJson(ex, reqType);
            Resp resp = handler.handle(ex, req);
            io.writeJson(ex, 200, resp);

        } catch (Throwable t) {
            writeMappedError(ex, t);
        } finally {
            io.safeClose(ex);
        }
    }

    private void writeMappedError(HttpExchange ex, Throwable t) {
        try {
            RestError err = errors.map(t);
            io.writeJson(ex, err.status(), err.payload());
        } catch (IOException ignored) {
        }
    }

    private static boolean isOptions(HttpExchange ex) {
        return "OPTIONS".equalsIgnoreCase(ex.getRequestMethod());
    }
}