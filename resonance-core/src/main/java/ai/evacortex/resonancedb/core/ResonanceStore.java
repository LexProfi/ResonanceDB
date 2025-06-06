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
    String insert(WavePattern psi, Map<String, String> metadata);
    void delete(String id);
    void update(String id, WavePattern psi, Map<String, String> metadata);
    float compare(WavePattern a, WavePattern b);
    List<ResonanceMatch> query(WavePattern query, int topK);
    List<ResonanceMatchDetailed> queryDetailed(WavePattern query, int topK);
    InterferenceMap queryInterference(WavePattern query, int topK);
    List<InterferenceEntry> queryInterferenceMap(WavePattern query, int topK);
}