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
import ai.evacortex.resonancedb.core.storage.io.CachedReader;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public final class FullScanCandidateSource implements CandidateSource {

    private final Function<String, CachedReader> readerProvider;
    private final Supplier<? extends Iterable<String>> segmentNamesSupplier;
    private final AtomicInteger lastSkippedSegments = new AtomicInteger();

    public FullScanCandidateSource(Supplier<? extends Iterable<String>> segmentNamesSupplier,
                                   Function<String, CachedReader> readerProvider) {
        this.segmentNamesSupplier = Objects.requireNonNull(segmentNamesSupplier);
        this.readerProvider = Objects.requireNonNull(readerProvider);
    }

    @Override
    public Map<String, Collection<String>> candidates(WavePattern query, int topK) {
        Map<String, Collection<String>> result = new LinkedHashMap<>();
        int skipped = 0;
        for (String segName : segmentNamesSupplier.get()) {
            CachedReader reader = readerProvider.apply(segName);
            if (reader == null) { skipped++; continue; }
            try {
                reader.acquire();
            } catch (IllegalStateException e) {
                skipped++;
                continue;
            }
            try {
                Collection<String> ids = reader.allIds();
                if (!ids.isEmpty()) {
                    result.put(segName, ids);
                }
            } finally {
                reader.release();
            }
        }
        lastSkippedSegments.set(skipped);
        return result;
    }

    @Override
    public ScoredCandidates scoredCandidates(WavePattern query, int topK) {
        Map<String, Collection<String>> bySegment = candidates(query, topK);
        boolean exhaustive = lastSkippedSegments.get() == 0;
        List<PostingSidecar.ScoredCandidate> flat = bySegment.values().stream()
                .flatMap(Collection::stream)
                .map(id -> new PostingSidecar.ScoredCandidate(id, 0.0f))
                .toList();
        return new ScoredCandidates(flat, exhaustive);
    }

    @Override
    public boolean isExhaustive() {
        return lastSkippedSegments.get() == 0;
    }

    public int getLastSkippedSegments() {
        return lastSkippedSegments.get();
    }
}
