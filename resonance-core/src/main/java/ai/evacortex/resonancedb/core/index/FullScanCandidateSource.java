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
import java.util.function.Function;
import java.util.function.Supplier;

public final class FullScanCandidateSource implements CandidateSource {

    private final Function<String, CachedReader> readerProvider;
    private final Supplier<? extends Iterable<String>> segmentNamesSupplier;

    public FullScanCandidateSource(Supplier<? extends Iterable<String>> segmentNamesSupplier,
                                   Function<String, CachedReader> readerProvider) {
        this.segmentNamesSupplier = Objects.requireNonNull(segmentNamesSupplier);
        this.readerProvider = Objects.requireNonNull(readerProvider);
    }

    @Override
    public Map<String, Collection<String>> candidates(WavePattern query, int topK) {
        Map<String, Collection<String>> result = new LinkedHashMap<>();
        for (String segName : segmentNamesSupplier.get()) {
            CachedReader reader = readerProvider.apply(segName);
            if (reader != null) {
                Collection<String> ids = reader.allIds();
                if (!ids.isEmpty()) {
                    result.put(segName, ids);
                }
            }
        }
        return result;
    }

    @Override
    public boolean isExhaustive() {
        return true;
    }
}
