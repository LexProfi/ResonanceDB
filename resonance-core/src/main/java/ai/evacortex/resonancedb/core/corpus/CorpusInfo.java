/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.corpus;


import java.time.Instant;
import java.util.Objects;

public record CorpusInfo(
        CorpusSpec spec,
        CorpusState state,
        long patternCount,
        Instant createdAt,
        Instant updatedAt
) {

    public CorpusInfo {
        spec = Objects.requireNonNull(spec, "spec must not be null");
        state = Objects.requireNonNull(state, "state must not be null");
        createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        updatedAt = Objects.requireNonNull(updatedAt, "updatedAt must not be null");

        if (patternCount < 0L) {
            throw new IllegalArgumentException("patternCount must be >= 0: " + patternCount);
        }
        if (updatedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException(
                    "updatedAt must be >= createdAt: updatedAt=" + updatedAt + ", createdAt=" + createdAt
            );
        }
    }

    public static CorpusInfo created(CorpusSpec spec, Instant now) {
        Objects.requireNonNull(spec, "spec must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return new CorpusInfo(spec, CorpusState.ACTIVE, 0L, now, now);
    }


    public String id() {
        return spec.id();
    }


    public int patternLength() {
        return spec.patternLength();
    }

    public boolean isReadable() {
        return state == CorpusState.ACTIVE || state == CorpusState.READ_ONLY;
    }

    public boolean isWritable() {
        return state == CorpusState.ACTIVE;
    }

    public CorpusInfo withState(CorpusState newState, Instant now) {
        Objects.requireNonNull(newState, "newState must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (now.isBefore(createdAt)) {
            throw new IllegalArgumentException("now must be >= createdAt: " + now);
        }
        return new CorpusInfo(spec, newState, patternCount, createdAt, max(updatedAt, now));
    }

    public CorpusInfo withPatternCount(long newPatternCount, Instant now) {
        Objects.requireNonNull(now, "now must not be null");
        if (newPatternCount < 0L) {
            throw new IllegalArgumentException("newPatternCount must be >= 0: " + newPatternCount);
        }
        if (now.isBefore(createdAt)) {
            throw new IllegalArgumentException("now must be >= createdAt: " + now);
        }
        return new CorpusInfo(spec, state, newPatternCount, createdAt, max(updatedAt, now));
    }

    public CorpusInfo withSpec(CorpusSpec newSpec, Instant now) {
        Objects.requireNonNull(newSpec, "newSpec must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (now.isBefore(createdAt)) {
            throw new IllegalArgumentException("now must be >= createdAt: " + now);
        }
        return new CorpusInfo(newSpec, state, patternCount, createdAt, max(updatedAt, now));
    }

    private static Instant max(Instant a, Instant b) {
        return a.isAfter(b) ? a : b;
    }
}