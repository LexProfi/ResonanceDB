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
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.rest.dto.*;
import ai.evacortex.resonancedb.rest.validation.WavePatternValidator;
import com.sun.net.httpserver.HttpExchange;

import java.util.Map;
import java.util.Objects;

public final class MutationHandlers {

    private final ResonanceStore store;
    private final WavePatternValidator validator;

    public MutationHandlers(ResonanceStore store, WavePatternValidator validator) {
        this.store = Objects.requireNonNull(store, "store");
        this.validator = Objects.requireNonNull(validator, "validator");
    }

    public IdResponse insert(HttpExchange ex, InsertRequest req)
            throws DuplicatePatternException, InvalidWavePatternException {

        WavePattern psi = validator.toWavePattern(req.pattern());
        Map<String, String> md = (req.metadata() == null) ? Map.of() : req.metadata();
        String id = store.insert(psi, md);
        return new IdResponse(id);
    }

    public IdResponse replace(HttpExchange ex, ReplaceRequest req)
            throws PatternNotFoundException, DuplicatePatternException, InvalidWavePatternException {

        WavePattern psi = validator.toWavePattern(req.pattern());
        Map<String, String> md = (req.metadata() == null) ? Map.of() : req.metadata();
        String id = store.replace(req.id(), psi, md);
        return new IdResponse(id);
    }

    public OkResponse delete(HttpExchange ex, DeleteRequest req)
            throws PatternNotFoundException {

        store.delete(req.id());
        return new OkResponse(true);
    }
}