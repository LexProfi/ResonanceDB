/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.corpus;

import ai.evacortex.resonancedb.core.ResonanceStore;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface CorpusService extends AutoCloseable {


    ResonanceStore store(String corpusId);

    Optional<CorpusInfo> info(String corpusId);

    boolean exists(String corpusId);

    List<CorpusInfo> list();

    @Override
    void close();

    static String normalizeCorpusId(String corpusId) {
        Objects.requireNonNull(corpusId, "corpusId must not be null");

        String normalized = corpusId.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("corpusId must not be blank");
        }
        if (normalized.length() > CorpusSpec.MAX_ID_LENGTH) {
            throw new IllegalArgumentException(
                    "corpusId length must be <= " + CorpusSpec.MAX_ID_LENGTH + ": " + normalized.length()
            );
        }

        for (int i = 0; i < normalized.length(); i++) {
            char c = normalized.charAt(i);
            boolean ok =
                    (c >= 'a' && c <= 'z') ||
                            (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') ||
                            c == '-' || c == '_' || c == '.';
            if (!ok) {
                throw new IllegalArgumentException(
                        "corpusId contains illegal character '" + c +
                                "'. Allowed: [A-Za-z0-9._-]"
                );
            }
        }

        return normalized;
    }
}