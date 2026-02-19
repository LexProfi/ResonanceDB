/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.handlers;

import ai.evacortex.resonancedb.core.ResonanceStore;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceEntry;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceMap;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;
import ai.evacortex.resonancedb.rest.dto.*;
import ai.evacortex.resonancedb.rest.util.TopK;
import ai.evacortex.resonancedb.rest.validation.WavePatternValidator;
import com.sun.net.httpserver.HttpExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class QueryHandlers {

    private final ResonanceStore store;
    private final WavePatternValidator validator;
    private final TopK topK;

    public QueryHandlers(ResonanceStore store, WavePatternValidator validator, TopK topK) {
        this.store = Objects.requireNonNull(store, "store");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.topK = Objects.requireNonNull(topK, "topK");
    }

    public CompareResponse compare(HttpExchange ex, CompareRequest req) {
        WavePattern a = validator.toWavePattern(req.a());
        WavePattern b = validator.toWavePattern(req.b());
        float score = store.compare(a, b);
        return new CompareResponse(score);
    }

    public List<ResonanceMatch> query(HttpExchange ex, QueryRequest req) {
        WavePattern q = validator.toWavePattern(req.query());
        int k = topK.clamp(req.topK());
        return store.query(q, k);
    }

    public List<ResonanceMatchDetailed> queryDetailed(HttpExchange ex, QueryRequest req) {
        WavePattern q = validator.toWavePattern(req.query());
        int k = topK.clamp(req.topK());
        return store.queryDetailed(q, k);
    }

    public InterferenceMap queryInterference(HttpExchange ex, QueryRequest req) {
        WavePattern q = validator.toWavePattern(req.query());
        int k = topK.clamp(req.topK());
        return store.queryInterference(q, k);
    }

    public List<InterferenceEntry> queryInterferenceMap(HttpExchange ex, QueryRequest req) {
        WavePattern q = validator.toWavePattern(req.query());
        int k = topK.clamp(req.topK());
        return store.queryInterferenceMap(q, k);
    }

    public List<ResonanceMatch> queryComposite(HttpExchange ex, CompositeQueryRequest req) throws InvalidWavePatternException {
        List<WavePattern> patterns = toPatterns(req.patterns());
        int k = topK.clamp(req.topK());
        return store.queryComposite(patterns, req.weights(), k);
    }

    public List<ResonanceMatchDetailed> queryCompositeDetailed(HttpExchange ex, CompositeQueryRequest req) throws InvalidWavePatternException {
        List<WavePattern> patterns = toPatterns(req.patterns());
        int k = topK.clamp(req.topK());
        return store.queryCompositeDetailed(patterns, req.weights(), k);
    }

    private List<WavePattern> toPatterns(List<WavePatternDto> dtos) {
        if (dtos == null) return List.of();
        List<WavePattern> patterns = new ArrayList<>(dtos.size());
        for (WavePatternDto p : dtos) {
            patterns.add(validator.toWavePattern(p));
        }
        return patterns;
    }
}