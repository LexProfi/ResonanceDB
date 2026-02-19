/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest;

import ai.evacortex.resonancedb.core.ResonanceStore;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.rest.dto.*;
import ai.evacortex.resonancedb.rest.error.ErrorMapper;
import ai.evacortex.resonancedb.rest.handlers.HealthHandlers;
import ai.evacortex.resonancedb.rest.handlers.MutationHandlers;
import ai.evacortex.resonancedb.rest.handlers.QueryHandlers;
import ai.evacortex.resonancedb.rest.http.RestPipeline;
import ai.evacortex.resonancedb.rest.http.RestRouter;
import ai.evacortex.resonancedb.rest.io.CorsSupport;
import ai.evacortex.resonancedb.rest.io.HttpIO;
import ai.evacortex.resonancedb.rest.json.JsonCodec;
import ai.evacortex.resonancedb.rest.json.ObjectMapperFactory;
import ai.evacortex.resonancedb.rest.util.TopK;
import ai.evacortex.resonancedb.rest.validation.WavePatternValidator;
import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ResonanceDBRest implements Closeable {

    private static final int MAX_BODY_BYTES = Integer.getInteger("resonance.rest.maxBodyBytes", 8 * 1024 * 1024);
    private final ResonanceStore store;
    private final Closeable storeCloseable;
    private final HttpServer server;
    private final ExecutorService httpExec;
    private final ResonanceRestConfig cfg;

    private final JsonCodec json;
    private final HttpIO io;
    private final CorsSupport cors;

    private final ErrorMapper errors;
    private final RestPipeline pipeline;
    private final RestRouter router;

    private final WavePatternValidator validator;
    private final TopK topK;

    private final HealthHandlers healthHandlers;
    private final QueryHandlers queryHandlers;
    private final MutationHandlers mutationHandlers;

    /**
     * Production ctor: HTTP executor is auto-selected (virtual threads).
     * <p>
     * IMPORTANT: we intentionally use virtual threads by default so HTTP never becomes the bottleneck;
     * the store limits parallelism internally (queryPool + ioLimiter).
     */
    public ResonanceDBRest(ResonanceStore store, Closeable storeCloseable, int port) throws IOException {
        this(store, storeCloseable, port, null);
    }

    /**
     * Internal/testing ctor: allows overriding executor (pass null for default).
     * Package-private by design (no external contract changes).
     */
    ResonanceDBRest(ResonanceStore store, Closeable storeCloseable, int port, ExecutorService executorOverride) throws IOException {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.storeCloseable = storeCloseable;
        this.cfg = ResonanceRestConfig.fromSystemProperties(port, MAX_BODY_BYTES);

        this.server = HttpServer.create(new InetSocketAddress(cfg.port()), 0);
        this.httpExec = (executorOverride != null)
                ? executorOverride
                : Executors.newVirtualThreadPerTaskExecutor();
        this.server.setExecutor(this.httpExec);

        this.json = new JsonCodec(ObjectMapperFactory.create());
        this.io = new HttpIO(json, cfg);
        this.cors = new CorsSupport(cfg);

        this.errors = new ErrorMapper();
        this.pipeline = new RestPipeline(io, cors, errors);
        this.router = new RestRouter(server, pipeline);

        this.validator = new WavePatternValidator(cfg);
        this.topK = new TopK(cfg);

        this.healthHandlers = new HealthHandlers();
        this.queryHandlers = new QueryHandlers(store, validator, topK);
        this.mutationHandlers = new MutationHandlers(store, validator);

        router.get("/health", ex -> io.writeJson(ex, 200, healthHandlers.health(ex)));
        router.postJson("/compare", CompareRequest.class, queryHandlers::compare);
        router.postJson("/query", QueryRequest.class, queryHandlers::query);
        router.postJson("/queryDetailed", QueryRequest.class, queryHandlers::queryDetailed);
        router.postJson("/queryInterference", QueryRequest.class, queryHandlers::queryInterference);
        router.postJson("/queryInterferenceMap", QueryRequest.class, queryHandlers::queryInterferenceMap);
        router.postJson("/queryComposite", CompositeQueryRequest.class, queryHandlers::queryComposite);
        router.postJson("/queryCompositeDetailed", CompositeQueryRequest.class, queryHandlers::queryCompositeDetailed);
        router.postJson("/insert", InsertRequest.class, mutationHandlers::insert);
        router.postJson("/replace", ReplaceRequest.class, mutationHandlers::replace);
        router.postJson("/delete", DeleteRequest.class, mutationHandlers::delete);
    }

    public static ResonanceDBRest withEmbeddedStore(Path dbRoot, int port) throws IOException {
        WavePatternStoreImpl impl = new WavePatternStoreImpl(dbRoot);
        return new ResonanceDBRest(impl, impl, port);
    }

    public void start() { server.start(); }

    public void stop(int delaySeconds) {
        try {
            server.stop(Math.max(0, delaySeconds));
        } finally {
            httpExec.shutdownNow();
            closeQuietly(storeCloseable);
        }
    }

    @Override
    public void close() { stop(0); }

    private static void closeQuietly(Closeable c) {
        if (c == null) return;
        try { c.close(); } catch (Exception ignored) {}
    }
}