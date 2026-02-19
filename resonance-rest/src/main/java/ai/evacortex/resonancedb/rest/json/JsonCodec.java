/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Objects;

public final class JsonCodec {

    private final ObjectMapper mapper;

    public JsonCodec(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public <T> T read(byte[] body, Class<T> type) throws IOException {
        Objects.requireNonNull(body, "body");
        Objects.requireNonNull(type, "type");
        return mapper.readValue(body, type);
    }

    public byte[] write(Object payload) throws JsonProcessingException {
        Objects.requireNonNull(payload, "payload");
        return mapper.writeValueAsBytes(payload);
    }

    public ObjectMapper raw() {
        return mapper;
    }
}