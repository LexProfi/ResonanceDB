/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.util;

import ai.evacortex.resonancedb.rest.ResonanceRestConfig;

import java.util.Objects;

public final class TopK {

    private final ResonanceRestConfig cfg;

    public TopK(ResonanceRestConfig cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    public int clamp(Integer topK) {
        int k = (topK == null) ? cfg.defaultTopK() : topK;
        if (k <= 0) return 0;
        return Math.min(k, cfg.maxTopK());
    }
}