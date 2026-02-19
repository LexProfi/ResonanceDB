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
import ai.evacortex.resonancedb.rest.error.BadRequestException;
import ai.evacortex.resonancedb.rest.json.JsonCodec;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public final class HttpIO {

    private final JsonCodec json;
    private final ResonanceRestConfig cfg;

    public HttpIO(JsonCodec json, ResonanceRestConfig cfg) {
        this.json = Objects.requireNonNull(json, "json");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    // =========================
    // JSON read/write
    // =========================

    public <T> T readJson(HttpExchange ex, Class<T> type) throws IOException {
        byte[] body = readBody(ex);
        if (body.length == 0) {
            throw new BadRequestException("Empty request body");
        }
        return json.read(body, type);
    }

    public void writeJson(HttpExchange ex, int status, Object payload) throws IOException {
        byte[] bytes = json.write(payload);

        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", "application/json; charset=utf-8");
        h.set("Cache-Control", "no-store");

        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
    }

    // =========================
    // Body handling
    // =========================

    private byte[] readBody(HttpExchange ex) throws IOException {
        long lenHeader = -1;
        String cl = ex.getRequestHeaders().getFirst("Content-Length");

        if (cl != null) {
            try {
                lenHeader = Long.parseLong(cl.trim());
            } catch (NumberFormatException ignored) {
            }
        }

        if (lenHeader > cfg.maxBodyBytes()) {
            throw new BadRequestException("Body too large");
        }

        try (InputStream in = ex.getRequestBody();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            byte[] buf = new byte[8192];
            int total = 0;
            int r;

            while ((r = in.read(buf)) >= 0) {
                total += r;
                if (total > cfg.maxBodyBytes()) {
                    throw new BadRequestException("Body too large");
                }
                out.write(buf, 0, r);
            }

            return out.toByteArray();
        }
    }

    // =========================
    // Exchange closing
    // =========================

    /**
     * Equivalent to original safeCloseExchange().
     */
    public void safeClose(HttpExchange ex) {
        try { ex.getRequestBody().close(); } catch (Exception ignored) {}
        try { ex.getResponseBody().close(); } catch (Exception ignored) {}
        try { ex.close(); } catch (Exception ignored) {}
    }
}