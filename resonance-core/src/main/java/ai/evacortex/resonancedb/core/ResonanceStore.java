/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
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
 * {@code ResonanceStore} defines the core contract for interacting with a cognitive waveform database
 * based on phase-aware semantic encoding. It supports content-addressable insertion, atomic replacement,
 * phase-sensitive queries, and interference-based retrieval over {@link WavePattern} instances.
 *
 * <p>Each pattern is uniquely identified by a fixed-length content-derived hash
 * (typically 16 bytes), ensuring deduplication, immutability, and reproducible addressing.
 * The exact hash algorithm (e.g. MD5, BLAKE2, xxHash) is implementation-defined and not contract-bound.</p>
 *
 * <p>Similarity is computed using constructive interference of complex waveforms ψ(x),
 * enabling fine-grained comparison of phase and amplitude alignment. Composite queries
 * and interference mapping support reasoning over superposed semantics.</p>
 *
 * <p>Implementations must be fully thread-safe, support concurrent reads and lock-isolated writes,
 * and guarantee deterministic behavior under identical input conditions.</p>
 *
 * @see WavePattern
 * @see ResonanceMatch
 * @see ResonanceMatchDetailed
 * @see InterferenceMap
 */
public interface ResonanceStore {

    /**
     * Inserts a new {@link WavePattern} into the store, indexed by its content-derived hash.
     *
     * @param psi      the wave pattern to store
     * @param metadata optional metadata associated with the pattern
     * @return a deterministic content-based hash (e.g., 16 bytes) serving as the unique identifier
     * @throws DuplicatePatternException     if a pattern with the same content already exists
     * @throws InvalidWavePatternException  if the pattern structure is invalid
     */
    String insert(WavePattern psi, Map<String, String> metadata);

    /**
     * Deletes a previously stored pattern by its content-based ID.
     *
     * @param id the content-derived hash of the pattern to delete
     * @throws PatternNotFoundException if no such pattern exists
     */
    void delete(String id);

    /**
     * Replaces an existing pattern by its ID and inserts a new wave pattern with optional metadata.
     *
     * <p>The new ID is deterministically derived from the new pattern's content.
     * This ensures content-addressable immutability and deduplication.</p>
     *
     * @param id    the ID of the existing pattern to remove
     * @param psi   the new {@link WavePattern} to insert
     * @param metadata optional metadata to associate
     * @return the content-derived ID of the new pattern
     * @throws PatternNotFoundException     if no pattern exists for the given ID
     * @throws InvalidWavePatternException  if the new pattern is invalid
     * @throws DuplicatePatternException    if the new pattern already exists
     */
    String replace(String id, WavePattern psi, Map<String, String> metadata);

    /**
     * Compares two wave patterns using the currently active resonance kernel.
     *
     * @param a first pattern
     * @param b second pattern
     * @return similarity score in [0.0 .. 1.0], where 1.0 indicates perfect constructive resonance
     */
    float compare(WavePattern a, WavePattern b);

    /**
     * Queries the store for the top-K most resonant matches to the given pattern.
     *
     * <p>Uses default comparison kernel and returns non-detailed match results optimized for ranking.</p>
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

    /**
     * Performs a composite query by superposing multiple wave patterns into a single
     * interference wave and returning top-K matches.
     *
     * @param patterns list of input wave patterns to combine
     * @param weights  optional list of weights; if {@code null}, uniform weighting is applied
     * @param topK     number of matches to return
     * @return list of {@link ResonanceMatch} results
     */
    List<ResonanceMatch> queryComposite(List<WavePattern> patterns, List<Double> weights, int topK);

    /**
     * Performs a detailed composite query, returning phase deltas, resonance zones, and scores.
     *
     * <p>This is useful for reasoning systems requiring fine-grained interpretability
     * of resonance across multiple semantic vectors.</p>
     *
     * @param patterns list of input wave patterns to combine
     * @param weights  optional weights (null = uniform)
     * @param topK     number of matches to return
     * @return list of {@link ResonanceMatchDetailed} with zone analysis
     */
    List<ResonanceMatchDetailed> queryCompositeDetailed(List<WavePattern> patterns, List<Double> weights, int topK);
}