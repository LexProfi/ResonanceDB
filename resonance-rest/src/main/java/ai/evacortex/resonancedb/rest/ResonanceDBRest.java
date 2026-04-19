/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest;

import ai.evacortex.resonancedb.core.corpus.CorpusService;
import ai.evacortex.resonancedb.core.storage.FileSystemCorpusService;
import ai.evacortex.resonancedb.rest.dto.CompareRequest;
import ai.evacortex.resonancedb.rest.dto.CompositeQueryRequest;
import ai.evacortex.resonancedb.rest.dto.DeleteRequest;
import ai.evacortex.resonancedb.rest.dto.InsertRequest;
import ai.evacortex.resonancedb.rest.dto.QueryRequest;
import ai.evacortex.resonancedb.rest.dto.ReplaceRequest;
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
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ResonanceDBRest implements Closeable {

    private static final int MAX_BODY_BYTES =
            Integer.getInteger("resonance.rest.maxBodyBytes", 8 * 1024 * 1024);

    private final CorpusService corpusService;
    private final AutoCloseable corpusServiceCloseable;
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

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public ResonanceDBRest(CorpusService corpusService, int port) throws IOException {
        this(corpusService, corpusService, port, null);
    }

    ResonanceDBRest(CorpusService corpusService,
                    AutoCloseable corpusServiceCloseable,
                    int port,
                    ExecutorService executorOverride) throws IOException {

        this.corpusService = Objects.requireNonNull(corpusService, "corpusService must not be null");
        this.corpusServiceCloseable = corpusServiceCloseable;
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
        this.queryHandlers = new QueryHandlers(corpusService, validator, topK);
        this.mutationHandlers = new MutationHandlers(corpusService, validator);

        router.get("/health", ex -> io.writeJson(ex, 200, healthHandlers.health(ex)));

        router.postJson("/corpora/{corpusId}/compare", CompareRequest.class, queryHandlers::compare);
        router.postJson("/corpora/{corpusId}/query", QueryRequest.class, queryHandlers::query);
        router.postJson("/corpora/{corpusId}/queryDetailed", QueryRequest.class, queryHandlers::queryDetailed);
        router.postJson("/corpora/{corpusId}/queryInterference", QueryRequest.class, queryHandlers::queryInterference);
        router.postJson("/corpora/{corpusId}/queryInterferenceMap", QueryRequest.class, queryHandlers::queryInterferenceMap);
        router.postJson("/corpora/{corpusId}/queryComposite", CompositeQueryRequest.class, queryHandlers::queryComposite);
        router.postJson("/corpora/{corpusId}/queryCompositeDetailed", CompositeQueryRequest.class, queryHandlers::queryCompositeDetailed);

        router.postJson("/corpora/{corpusId}/insert", InsertRequest.class, mutationHandlers::insert);
        router.postJson("/corpora/{corpusId}/replace", ReplaceRequest.class, mutationHandlers::replace);
        router.postJson("/corpora/{corpusId}/delete", DeleteRequest.class, mutationHandlers::delete);
    }

    public static ResonanceDBRest withEmbeddedStore(Path dbRoot, int port) throws IOException {
        FileSystemCorpusService corpora = new FileSystemCorpusService(dbRoot);
        return new ResonanceDBRest(corpora, corpora, port, null);
    }

    public void start() {
        server.start();
    }

    public void stop(int delaySeconds) {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        try {
            server.stop(Math.max(0, delaySeconds));
        } finally {
            httpExec.shutdownNow();
            closeQuietly(corpusServiceCloseable);
        }
    }

    @Override
    public void close() {
        stop(0);
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception ignored) {
        }
    }
}