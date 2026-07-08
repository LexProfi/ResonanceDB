/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface CandidateSource {

    Map<String, Collection<String>> candidates(WavePattern query, int topK);

    default ScoredCandidates scoredCandidates(WavePattern query, int topK) {
        Map<String, Collection<String>> bySegment = candidates(query, topK);
        List<PostingSidecar.ScoredCandidate> flat = bySegment.values().stream()
                .flatMap(Collection::stream)
                .map(id -> new PostingSidecar.ScoredCandidate(id, 0.0f))
                .toList();
        return new ScoredCandidates(flat, true);
    }

    boolean isExhaustive();

    record ScoredCandidates(List<PostingSidecar.ScoredCandidate> candidates, boolean exhaustive) {}
}
