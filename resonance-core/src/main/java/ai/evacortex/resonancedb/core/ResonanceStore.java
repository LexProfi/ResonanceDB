/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core;

import java.util.List;
import java.util.Map;

public interface ResonanceStore {
    void insert(String id, WavePattern psi, Map<String, String> metadata);
    String insert(WavePattern psi);
    void delete(String id);
    void update(String id, WavePattern psi);
    List<ResonanceMatch> query(WavePattern query, int topK);
    float compare(WavePattern a, WavePattern b);
}