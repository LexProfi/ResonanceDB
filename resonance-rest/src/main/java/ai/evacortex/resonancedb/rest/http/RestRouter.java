/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public final class RestRouter {

    private static final String ATTR_ROUTE_PARAM_PREFIX = "resonance.rest.route.param.";

    private final HttpServer server;
    private final RestPipeline pipeline;
    private final List<Route> routes = new CopyOnWriteArrayList<>();

    public RestRouter(HttpServer server, RestPipeline pipeline) {
        this.server = Objects.requireNonNull(server, "server");
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
        this.server.createContext("/", this::dispatch);
    }

    public void get(String path, ExchangeHandlers.ExchangeHandler handler) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(handler, "handler");
        routes.add(Route.forGet(path, handler));
    }

    public void post(String path, ExchangeHandlers.ExchangeHandler handler) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(handler, "handler");
        routes.add(Route.forPost(path, handler));
    }

    public <Req, Resp> void postJson(
            String path,
            Class<Req> reqType,
            ExchangeHandlers.ExchangeJsonHandler<Req, Resp> handler
    ) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(reqType, "reqType");
        Objects.requireNonNull(handler, "handler");
        routes.add(Route.forPostJson(path, reqType, handler));
    }

    public static String pathParam(HttpExchange ex, String name) {
        Objects.requireNonNull(ex, "exchange");
        Objects.requireNonNull(name, "name");
        Object value = ex.getAttribute(ATTR_ROUTE_PARAM_PREFIX + name);
        if (value == null) {
            throw new IllegalArgumentException("Missing route parameter: " + name);
        }
        return value.toString();
    }

    private void dispatch(HttpExchange ex) throws IOException {
        try {
            String requestPath = normalizePath(ex.getRequestURI().getPath());
            String method = normalizeMethod(ex.getRequestMethod());

            Route best = null;
            RouteMatch bestMatch = null;

            for (Route route : routes) {
                if (!route.supportsMethod(method)) {
                    continue;
                }

                RouteMatch match = route.path.match(requestPath);
                if (!match.matched()) {
                    continue;
                }

                if (best == null || match.isBetterThan(bestMatch)) {
                    best = route;
                    bestMatch = match;
                }
            }

            if (best == null) {
                writeNotFound(ex);
                return;
            }

            for (Map.Entry<String, String> e : bestMatch.params().entrySet()) {
                ex.setAttribute(ATTR_ROUTE_PARAM_PREFIX + e.getKey(), e.getValue());
            }

            switch (best.kind) {
                case GET -> pipeline.handleGet(ex, best.exchangeHandler);
                case POST -> pipeline.handlePost(ex, best.exchangeHandler);
                case POST_JSON -> dispatchPostJson(best, ex);
                default -> throw new IllegalStateException("Unsupported route kind: " + best.kind);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Route dispatch failed", e);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void dispatchPostJson(Route route, HttpExchange ex) throws Exception {
        pipeline.handlePostJson(ex, (Class) route.reqType, (ExchangeHandlers.ExchangeJsonHandler) route.jsonHandler);
    }

    private static void writeNotFound(HttpExchange ex) throws IOException {
        byte[] body = "{\"code\":\"not_found\",\"message\":\"Route not found\"}".getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(404, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    private static String normalizeMethod(String method) {
        if (method == null) {
            return "";
        }
        return method.trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "/";
        }

        String p = path.trim();
        if (!p.startsWith("/")) {
            p = "/" + p;
        }

        while (p.length() > 1 && p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }

        return p;
    }

    private enum RouteKind {
        GET,
        POST,
        POST_JSON
    }

    private static final class Route {
        private final RouteKind kind;
        private final CompiledPath path;
        private final ExchangeHandlers.ExchangeHandler exchangeHandler;
        private final Class<?> reqType;
        private final ExchangeHandlers.ExchangeJsonHandler<?, ?> jsonHandler;

        private Route(RouteKind kind,
                      CompiledPath path,
                      ExchangeHandlers.ExchangeHandler exchangeHandler,
                      Class<?> reqType,
                      ExchangeHandlers.ExchangeJsonHandler<?, ?> jsonHandler) {
            this.kind = kind;
            this.path = path;
            this.exchangeHandler = exchangeHandler;
            this.reqType = reqType;
            this.jsonHandler = jsonHandler;
        }

        static Route forGet(String path, ExchangeHandlers.ExchangeHandler handler) {
            return new Route(RouteKind.GET, CompiledPath.compile(path), handler, null, null);
        }

        static Route forPost(String path, ExchangeHandlers.ExchangeHandler handler) {
            return new Route(RouteKind.POST, CompiledPath.compile(path), handler, null, null);
        }

        static <Req, Resp> Route forPostJson(String path,
                                             Class<Req> reqType,
                                             ExchangeHandlers.ExchangeJsonHandler<Req, Resp> handler) {
            return new Route(RouteKind.POST_JSON, CompiledPath.compile(path), null, reqType, handler);
        }

        boolean supportsMethod(String method) {
            if ("OPTIONS".equals(method)) {
                return true;
            }
            return switch (kind) {
                case GET -> "GET".equals(method);
                case POST, POST_JSON -> "POST".equals(method);
            };
        }
    }

    private record RouteMatch(boolean matched,
                              int literalCount,
                              int segmentCount,
                              Map<String, String> params) {

        boolean isBetterThan(RouteMatch other) {
            if (other == null) {
                return true;
            }
            if (literalCount != other.literalCount) {
                return literalCount > other.literalCount;
            }
            return segmentCount > other.segmentCount;
        }

        static RouteMatch no() {
            return new RouteMatch(false, -1, -1, Map.of());
        }
    }

    private static final class CompiledPath {
        private final String template;
        private final List<PathSegment> segments;

        private CompiledPath(String template, List<PathSegment> segments) {
            this.template = template;
            this.segments = segments;
        }

        static CompiledPath compile(String template) {
            String normalized = normalizePath(template);
            if ("/".equals(normalized)) {
                return new CompiledPath(normalized, List.of());
            }

            String[] raw = normalized.substring(1).split("/");
            List<PathSegment> segments = new ArrayList<>(raw.length);
            for (String token : raw) {
                if (token.startsWith("{") && token.endsWith("}") && token.length() > 2) {
                    segments.add(PathSegment.param(token.substring(1, token.length() - 1)));
                } else {
                    segments.add(PathSegment.literal(token));
                }
            }
            return new CompiledPath(normalized, List.copyOf(segments));
        }

        RouteMatch match(String requestPath) {
            String normalized = normalizePath(requestPath);

            if (segments.isEmpty()) {
                return "/".equals(normalized)
                        ? new RouteMatch(true, 0, 0, Map.of())
                        : RouteMatch.no();
            }

            if ("/".equals(normalized)) {
                return RouteMatch.no();
            }

            String[] raw = normalized.substring(1).split("/");
            if (raw.length != segments.size()) {
                return RouteMatch.no();
            }

            Map<String, String> params = new LinkedHashMap<>();
            int literalCount = 0;

            for (int i = 0; i < raw.length; i++) {
                String actual = raw[i];
                PathSegment expected = segments.get(i);

                if (expected.literal) {
                    if (!expected.value.equals(actual)) {
                        return RouteMatch.no();
                    }
                    literalCount++;
                } else {
                    if (actual.isEmpty()) {
                        return RouteMatch.no();
                    }
                    params.put(expected.value, actual);
                }
            }

            return new RouteMatch(true, literalCount, segments.size(), Map.copyOf(params));
        }

        @Override
        public String toString() {
            return template;
        }
    }

    private static final class PathSegment {
        private final boolean literal;
        private final String value;

        private PathSegment(boolean literal, String value) {
            this.literal = literal;
            this.value = value;
        }

        static PathSegment literal(String value) {
            return new PathSegment(true, value);
        }

        static PathSegment param(String name) {
            return new PathSegment(false, name);
        }
    }
}