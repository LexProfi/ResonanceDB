/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest;

import java.util.Objects;

/**
 * Immutable REST configuration for {@link ResonanceDBRest}.
 *
 * <p><b>Contract safety:</b> defaults are chosen to preserve the behavior of the original single-class
 * implementation (same max body bytes knob, same topK defaults/clamps, same permissive CORS).
 *
 * <p><b>Threading:</b> this config does not control the HTTP executor; that is intentionally kept in
 * {@link ResonanceDBRest} to preserve the virtual-thread-per-request approach and store-side load balancing.
 */
public record ResonanceRestConfig(
        int port,
        int maxBodyBytes,
        String corsAllowOrigin,
        String corsAllowMethods,
        String corsAllowHeaders,
        int defaultTopK,
        int maxTopK,
        boolean validateFiniteWaveValues,
        int patternLen
) {

    public ResonanceRestConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be in range [0..65535], got: " + port);
        }
        if (maxBodyBytes <= 0) {
            throw new IllegalArgumentException("maxBodyBytes must be > 0, got: " + maxBodyBytes);
        }
        corsAllowOrigin = Objects.requireNonNull(corsAllowOrigin, "corsAllowOrigin");
        corsAllowMethods = Objects.requireNonNull(corsAllowMethods, "corsAllowMethods");
        corsAllowHeaders = Objects.requireNonNull(corsAllowHeaders, "corsAllowHeaders");

        if (defaultTopK < 0) {
            throw new IllegalArgumentException("defaultTopK must be >= 0, got: " + defaultTopK);
        }
        if (maxTopK <= 0) {
            throw new IllegalArgumentException("maxTopK must be > 0, got: " + maxTopK);
        }
        if (defaultTopK > maxTopK) {
            throw new IllegalArgumentException("defaultTopK must be <= maxTopK, got defaultTopK="
                    + defaultTopK + " maxTopK=" + maxTopK);
        }

        if (patternLen <= 0) {
            throw new IllegalArgumentException("patternLen must be > 0, got: " + patternLen);
        }
    }

    public static ResonanceRestConfig fromSystemProperties(int port, int maxBodyBytesDefault) {
        int maxBody = getInt("resonance.rest.maxBodyBytes", maxBodyBytesDefault);
        if (maxBody <= 0) maxBody = maxBodyBytesDefault;

        String corsOrigin = getString("resonance.rest.corsOrigin", "*");
        String corsMethods = getString("resonance.rest.corsMethods", "GET,POST,OPTIONS");
        String corsHeaders = getString("resonance.rest.corsHeaders", "Content-Type");

        int defTopK = getInt("resonance.rest.topK.default", 10);
        if (defTopK < 0) defTopK = 10;

        int maxTopK = getInt("resonance.rest.topK.max", 10_000);
        if (maxTopK <= 0) maxTopK = 10_000;

        if (defTopK > maxTopK) defTopK = maxTopK;

        boolean validateFinite = getBool("resonance.rest.validateFiniteWaveValues", false);

        int patternLen = getInt("resonance.pattern.len", 1536);
        if (patternLen <= 0) patternLen = 1536;

        return new ResonanceRestConfig(
                port,
                maxBody,
                corsOrigin,
                corsMethods,
                corsHeaders,
                defTopK,
                maxTopK,
                validateFinite,
                patternLen
        );
    }

    private static int getInt(String key, int def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        if (v.isEmpty()) return def;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException ignored) {
            return def;
        }
    }

    private static boolean getBool(String key, boolean def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        if (v.isEmpty()) return def;
        if ("true".equalsIgnoreCase(v) || "1".equals(v) || "yes".equalsIgnoreCase(v) || "y".equalsIgnoreCase(v)) {
            return true;
        }
        if ("false".equalsIgnoreCase(v) || "0".equals(v) || "no".equalsIgnoreCase(v) || "n".equalsIgnoreCase(v)) {
            return false;
        }
        return def;
    }

    private static String getString(String key, String def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        return v.isEmpty() ? def : v;
    }
}