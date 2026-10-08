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
import ai.evacortex.resonancedb.core.corpus.CorpusService;
import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.PhaseWeights;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceEntry;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceMap;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;
import ai.evacortex.resonancedb.rest.dto.*;
import ai.evacortex.resonancedb.rest.error.BadRequestException;
import ai.evacortex.resonancedb.rest.http.RestRouter;
import ai.evacortex.resonancedb.rest.util.TopK;
import ai.evacortex.resonancedb.rest.validation.WavePatternValidator;
import com.sun.net.httpserver.HttpExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public final class QueryHandlers {

    private final CorpusService corpora;
    private final WavePatternValidator validator;
    private final TopK topK;

    public QueryHandlers(CorpusService corpora, WavePatternValidator validator, TopK topK) {
        this.corpora = Objects.requireNonNull(corpora, "corpora");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.topK = Objects.requireNonNull(topK, "topK");
    }

    public CompareResponse compare(HttpExchange ex, CompareRequest req) {
        ResonanceStore store = resolveStore(ex);
        WavePattern a = validator.toWavePattern(req.a());
        WavePattern b = validator.toWavePattern(req.b());
        CompareOptions options = toCompareOptions(req.phaseWeights(), a.amplitude().length);
        float score = (options != null)
                ? store.compare(a, b, options)
                : store.compare(a, b);
        return new CompareResponse(score);
    }

    public List<ResonanceMatch> query(HttpExchange ex, QueryRequest req) {
        ResonanceStore store = resolveStore(ex);
        WavePattern q = validator.toWavePattern(req.query());
        int k = topK.clamp(req.topK());
        CompareOptions options = toCompareOptions(req.phaseWeights(), q.amplitude().length);
        if (options != null) {
            return store.query(q, k, options);
        }
        return store.query(q, k);
    }

    public List<ResonanceMatchDetailed> queryDetailed(HttpExchange ex, QueryRequest req) {
        ResonanceStore store = resolveStore(ex);
        WavePattern q = validator.toWavePattern(req.query());
        int k = topK.clamp(req.topK());
        CompareOptions options = toCompareOptions(req.phaseWeights(), q.amplitude().length);
        if (options != null) {
            return store.queryDetailed(q, k, options);
        }
        return store.queryDetailed(q, k);
    }

    public InterferenceMap queryInterference(HttpExchange ex, QueryRequest req) {
        ResonanceStore store = resolveStore(ex);
        WavePattern q = validator.toWavePattern(req.query());
        int k = topK.clamp(req.topK());
        return store.queryInterference(q, k);
    }

    public List<InterferenceEntry> queryInterferenceMap(HttpExchange ex, QueryRequest req) {
        ResonanceStore store = resolveStore(ex);
        WavePattern q = validator.toWavePattern(req.query());
        int k = topK.clamp(req.topK());
        return store.queryInterferenceMap(q, k);
    }

    public List<ResonanceMatch> queryComposite(HttpExchange ex, CompositeQueryRequest req)
            throws InvalidWavePatternException {
        ResonanceStore store = resolveStore(ex);
        List<WavePattern> patterns = toPatterns(req.patterns());
        validateWeights(req.weights(), patterns.size());
        int k = topK.clamp(req.topK());
        return store.queryComposite(patterns, req.weights(), k);
    }

    public List<ResonanceMatchDetailed> queryCompositeDetailed(HttpExchange ex, CompositeQueryRequest req)
            throws InvalidWavePatternException {
        ResonanceStore store = resolveStore(ex);
        List<WavePattern> patterns = toPatterns(req.patterns());
        validateWeights(req.weights(), patterns.size());
        int k = topK.clamp(req.topK());
        return store.queryCompositeDetailed(patterns, req.weights(), k);
    }

    static CompareOptions toCompareOptions(PhaseWeightsDto dto, int expectedDimension) {
        if (dto == null || dto.weights == null) {
            return null;
        }
        validatePhaseWeights(dto.weights, expectedDimension);
        return CompareOptions.withPhaseWeights(new PhaseWeights(dto.weights));
    }

    private static void validatePhaseWeights(double[] weights, int expectedDimension) {
        if (weights.length == 0) {
            throw new BadRequestException("phaseWeights.weights must not be empty");
        }
        if (weights.length != expectedDimension) {
            throw new BadRequestException(
                    "phaseWeights.weights length (" + weights.length
                            + ") must match pattern dimension (" + expectedDimension + ")");
        }
        for (int i = 0; i < weights.length; i++) {
            double w = weights[i];
            if (!Double.isFinite(w)) {
                throw new BadRequestException("phaseWeights.weights[" + i + "] must be a finite number");
            }
            if (w < 0.0 || w > 1.0) {
                throw new BadRequestException(
                        "phaseWeights.weights[" + i + "] = " + w + " is out of range [0.0, 1.0]");
            }
        }
    }

    private static void validateWeights(List<Double> weights, int patternCount) {
        if (weights == null || weights.isEmpty()) {
            throw new BadRequestException("'weights' is required");
        }
        if (weights.size() != patternCount) {
            throw new BadRequestException(
                    "weights length (" + weights.size() + ") must match patterns length (" + patternCount + ")");
        }
        for (int i = 0; i < weights.size(); i++) {
            if (weights.get(i) == null || !Double.isFinite(weights.get(i))) {
                throw new BadRequestException(
                        "weight at index " + i + " must be a finite number");
            }
        }
    }

    private ResonanceStore resolveStore(HttpExchange ex) {
        String corpusId = RestRouter.pathParam(ex, "corpusId");
        return corpora.store(corpusId);
    }

    private List<WavePattern> toPatterns(List<WavePatternDto> dtos) {
        if (dtos == null || dtos.isEmpty()) {
            return List.of();
        }

        List<WavePattern> patterns = new ArrayList<>(dtos.size());
        for (WavePatternDto dto : dtos) {
            patterns.add(validator.toWavePattern(dto));
        }
        return patterns;
    }
}
