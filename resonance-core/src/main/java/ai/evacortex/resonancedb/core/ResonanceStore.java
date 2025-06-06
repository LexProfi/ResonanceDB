/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.exceptions.*;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceEntry;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceMap;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;

import java.util.List;
import java.util.Map;

/**
 * {@code ResonanceStore} defines the contract for interacting with a persistent cognitive
 * waveform database that supports insertions, deletions, updates, and semantic queries
 * over {@link WavePattern} instances.
 *
 * <p>Each stored pattern is uniquely identified by a deterministic MD5 hash computed
 * from its contents. Queries are evaluated using the principle of constructive interference
 * between waveforms.</p>
 *
 * <p>Implementations must ensure thread safety and atomicity of updates, and support
 * concurrent read/write access. All results are expected to be deterministic and
 * reproducible under stable inputs.</p>
 *
 * @see WavePattern
 * @see ResonanceMatch
 * @see ResonanceMatchDetailed
 * @see InterferenceMap
 */
public interface ResonanceStore {

    /**
     * Inserts a new {@link WavePattern} into the store, indexed by its content hash.
     *
     * @param psi      the wave pattern to store
     * @param metadata optional metadata associated with the pattern
     * @return unique MD5-based identifier for the pattern
     * @throws DuplicatePatternException     if a pattern with the same content already exists
     * @throws InvalidWavePatternException  if the pattern structure is invalid
     */
    String insert(WavePattern psi, Map<String, String> metadata);

    /**
     * Deletes a previously stored pattern by its ID.
     *
     * @param id the content-based ID of the pattern
     * @throws PatternNotFoundException if no such pattern exists
     */
    void delete(String id);

    /**
     * Updates an existing pattern with a new waveform and metadata.
     *
     * <p>The ID must match an existing pattern, and the content will be
     * completely replaced (not merged).</p>
     *
     * @param id       the content hash ID of the existing pattern
     * @param psi      the new pattern to replace the old one
     * @param metadata updated metadata to associate
     * @throws PatternNotFoundException     if no such ID exists
     * @throws InvalidWavePatternException  if the new pattern is invalid
     */
    void update(String id, WavePattern psi, Map<String, String> metadata);

    /**
     * Compares two wave patterns using the currently active resonance kernel.
     *
     * @param a first pattern
     * @param b second pattern
     * @return similarity score in [0.0 .. 1.0] according to resonance metrics
     */
    float compare(WavePattern a, WavePattern b);

    /**
     * Queries the store for the top-K most resonant matches to the given pattern.
     *
     * <p>Uses default comparison options and returns lightweight match results.</p>
     *
     * @param query the input pattern
     * @param topK  the number of top matches to return
     * @return list of {@link ResonanceMatch}, ordered by descending similarity
     */
    List<ResonanceMatch> query(WavePattern query, int topK);

    /**
     * Queries the store and returns detailed match results, including phase deltas and zones.
     *
     * <p>This is typically used for semantic zone classification or diagnostics.</p>
     *
     * @param query the input pattern
     * @param topK  the number of top detailed matches to return
     * @return list of {@link ResonanceMatchDetailed} results
     */
    List<ResonanceMatchDetailed> queryDetailed(WavePattern query, int topK);

    /**
     * Computes a high-level interference map for the query pattern, aggregating detailed results.
     *
     * @param query the input pattern
     * @param topK  the number of matches to consider
     * @return structured {@link InterferenceMap} for visualization or analysis
     */
    InterferenceMap queryInterference(WavePattern query, int topK);

    /**
     * Returns a flat list of interference entries including energy and phase shift info.
     *
     * <p>This is a lightweight variant of {@link #queryInterference(WavePattern, int)}.</p>
     *
     * @param query the input pattern
     * @param topK  number of top entries
     * @return list of {@link InterferenceEntry} instances
     */
    List<InterferenceEntry> queryInterferenceMap(WavePattern query, int topK);
}