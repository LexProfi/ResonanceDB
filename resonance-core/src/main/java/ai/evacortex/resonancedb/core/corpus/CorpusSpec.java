/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.corpus;

import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record CorpusSpec(
        String id,
        int patternLength,
        Map<String, String> attributes
) {

    public static final int MAX_ID_LENGTH = 128;

    public CorpusSpec(String id, int patternLength) {
        this(id, patternLength, Map.of());
    }

    public CorpusSpec {
        id = normalizeAndValidateId(id);
        patternLength = validatePatternLength(patternLength);
        attributes = normalizeAttributes(attributes);
    }

    public static CorpusSpec fromFirstWrite(String corpusId, WavePattern pattern) {
        Objects.requireNonNull(pattern, "pattern must not be null");
        validatePattern(pattern);
        return new CorpusSpec(corpusId, pattern.amplitude().length);
    }

    public boolean matches(WavePattern pattern) {
        if (pattern == null) {
            return false;
        }
        double[] amplitude = pattern.amplitude();
        double[] phase = pattern.phase();
        return amplitude != null
                && phase != null
                && amplitude.length == phase.length
                && amplitude.length == patternLength;
    }

    public void requireCompatible(WavePattern pattern) {
        Objects.requireNonNull(pattern, "pattern must not be null");
        validatePattern(pattern);
        if (pattern.amplitude().length != patternLength) {
            throw new IllegalArgumentException(
                    "Pattern length " + pattern.amplitude().length +
                            " does not match corpus patternLength " + patternLength +
                            " for corpus '" + id + '\''
            );
        }
    }

    private static String normalizeAndValidateId(String id) {
        if (id == null) {
            throw new NullPointerException("corpus id must not be null");
        }

        String normalized = id.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("corpus id must not be blank");
        }
        if (normalized.length() > MAX_ID_LENGTH) {
            throw new IllegalArgumentException(
                    "corpus id length must be <= " + MAX_ID_LENGTH + ": " + normalized.length()
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
                        "corpus id contains illegal character '" + c +
                                "'. Allowed: [A-Za-z0-9._-]"
                );
            }
        }

        return normalized;
    }

    private static int validatePatternLength(int patternLength) {
        if (patternLength <= 0) {
            throw new IllegalArgumentException("patternLength must be > 0: " + patternLength);
        }
        return patternLength;
    }

    private static Map<String, String> normalizeAttributes(Map<String, String> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return Map.of();
        }

        LinkedHashMap<String, String> copy = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : attributes.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();

            if (key == null) {
                throw new IllegalArgumentException("attribute key must not be null");
            }
            if (value == null) {
                throw new IllegalArgumentException("attribute value must not be null for key: " + key);
            }

            String normalizedKey = key.trim();
            String normalizedValue = value.trim();

            if (normalizedKey.isEmpty()) {
                throw new IllegalArgumentException("attribute key must not be blank");
            }

            copy.put(normalizedKey, normalizedValue);
        }
        return Map.copyOf(copy);
    }

    private static void validatePattern(WavePattern pattern) {
        double[] amplitude = pattern.amplitude();
        double[] phase = pattern.phase();

        if (amplitude == null) {
            throw new IllegalArgumentException("pattern amplitude must not be null");
        }
        if (phase == null) {
            throw new IllegalArgumentException("pattern phase must not be null");
        }
        if (amplitude.length == 0) {
            throw new IllegalArgumentException("pattern amplitude must not be empty");
        }
        if (phase.length == 0) {
            throw new IllegalArgumentException("pattern phase must not be empty");
        }
        if (amplitude.length != phase.length) {
            throw new IllegalArgumentException(
                    "pattern amplitude/phase length mismatch: " +
                            amplitude.length + " != " + phase.length
            );
        }
    }
}