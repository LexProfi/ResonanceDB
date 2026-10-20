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
import ai.evacortex.resonancedb.rest.json.JsonCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

class HttpIOSafeCloseTest {

    private HttpIO io;

    @BeforeEach
    void setUp() {
        JsonCodec json = new JsonCodec(new ObjectMapper().findAndRegisterModules());
        ResonanceRestConfig cfg = ResonanceRestConfig.fromSystemProperties(31415, 4 * 1024 * 1024);
        io = new HttpIO(json, cfg);
    }

    @Test
    @DisplayName("safeClose handles all streams throwing without propagating exceptions")
    void safeCloseSwallowsAllExceptions() {
        HttpExchange ex = new StubExchange(
                new BrokenInputStream(),
                new BrokenOutputStream()
        );

        assertDoesNotThrow(() -> io.safeClose(ex),
                "safeClose must never propagate exceptions");
    }

    @Test
    @DisplayName("safeClose handles 'insufficient bytes written to stream' scenario")
    void safeCloseHandlesInsufficientBytes() {
        HttpExchange ex = new StubExchange(
                new ByteArrayInputStream(new byte[0]),
                new OutputStream() {
                    @Override public void write(int b) throws IOException {
                        throw new IOException("insufficient bytes written to stream");
                    }
                    @Override public void close() throws IOException {
                        throw new IOException("insufficient bytes written to stream");
                    }
                }
        );

        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured));
        try {
            assertDoesNotThrow(() -> io.safeClose(ex),
                    "safeClose must handle 'insufficient bytes' gracefully");
            assertTrue(captured.toString().contains("insufficient bytes written to stream"),
                    "Error should be logged to stderr");
        } finally {
            System.setErr(originalErr);
        }
    }

    @Test
    @DisplayName("safeClose works normally when all streams are healthy")
    void safeCloseHealthyStreams() {
        HttpExchange ex = new StubExchange(
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream()
        );

        assertDoesNotThrow(() -> io.safeClose(ex));
    }

    private static class BrokenInputStream extends InputStream {
        @Override public int read() throws IOException {
            throw new IOException("broken");
        }
        @Override public void close() throws IOException {
            throw new IOException("broken close");
        }
    }

    private static class BrokenOutputStream extends OutputStream {
        @Override public void write(int b) throws IOException {
            throw new IOException("broken");
        }
        @Override public void close() throws IOException {
            throw new IOException("broken close");
        }
    }

    private static class StubExchange extends HttpExchange {
        private final InputStream requestBody;
        private final OutputStream responseBody;

        StubExchange(InputStream requestBody, OutputStream responseBody) {
            this.requestBody = requestBody;
            this.responseBody = responseBody;
        }

        @Override public InputStream getRequestBody() { return requestBody; }
        @Override public OutputStream getResponseBody() { return responseBody; }
        @Override public void close() {}

        @Override public Headers getRequestHeaders() { return new Headers(); }
        @Override public Headers getResponseHeaders() { return new Headers(); }
        @Override public URI getRequestURI() { return URI.create("/"); }
        @Override public String getRequestMethod() { return "GET"; }
        @Override public java.net.InetSocketAddress getRemoteAddress() { return null; }
        @Override public java.net.InetSocketAddress getLocalAddress() { return null; }
        @Override public String getProtocol() { return "HTTP/1.1"; }
        @Override public int getResponseCode() { return 200; }
        @Override public com.sun.net.httpserver.HttpContext getHttpContext() { return null; }
        @Override public Object getAttribute(String name) { return null; }
        @Override public void setAttribute(String name, Object value) {}
        @Override public void sendResponseHeaders(int rCode, long responseLength) {}
        @Override public void setStreams(InputStream i, OutputStream o) {}
        @Override public com.sun.net.httpserver.HttpPrincipal getPrincipal() { return null; }
    }
}
