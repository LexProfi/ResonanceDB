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
 * {@code ResonanceStore} defines the contract for interacting with a persistent,
 * phase-aware cognitive waveform database. It supports content-addressable insertions,
 * deletions, replacements, and semantic resonance queries over {@link WavePattern} instances.
 *
 * <p>Each stored pattern is uniquely identified by a deterministic 16-byte MD5 hash
 * computed from its contents. This ID remains immutable and guarantees deduplication,
 * idempotent storage, and consistent referencing across sessions.</p>
 *
 * <p>Queries are evaluated using the principle of constructive interference between waveforms,
 * enabling phase-sensitive similarity, superposition-based composite reasoning,
 * and energy-weighted semantic retrieval. The system operates on ψ(x)-encoded patterns
 * and does not rely on symbolic keys.</p>
 *
 * <p>Implementations must ensure full thread safety and atomicity of insert, replace, and delete
 * operations. Read/write concurrency must be handled via lock-isolated mutation.
 * All comparison and query operations must be deterministic and reproducible
 * under identical input conditions.</p>
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
     * @return the 16-byte content-derived MD5 hash that serves as the unique identifier
     * @throws DuplicatePatternException     if a pattern with the same content already exists
     * @throws InvalidWavePatternException  if the pattern structure is invalid
     */
    String insert(WavePattern psi, Map<String, String> metadata);

    /**
     * Deletes a previously stored pattern by its ID.
     *
     * @param id the 16-byte MD5 content hash of the pattern to delete
     * @throws PatternNotFoundException if no such pattern exists
     */
    void delete(String id);

    /**
     * Replaces an existing pattern by deleting the pattern associated with the given ID
     * and inserting a new waveform pattern with updated metadata.
     *
     * <p>The original ID is used to locate and remove the existing pattern.
     * The new pattern is then inserted as a completely separate entry,
     * and its ID is computed from its content (MD5 hash).</p>
     *
     * <p>This method ensures consistency in content-addressable storage,
     * where the ID always reflects the actual content of the pattern.</p>
     *
     * @param id the ID of the existing pattern to remove (must match an existing entry)
     * @param psi the new {@link WavePattern} to insert
     * @return the 16-byte content-derived MD5 hash that serves as the unique identifier
     * @throws PatternNotFoundException     if no pattern exists for the given ID
     * @throws InvalidWavePatternException  if the new pattern is invalid
     * @throws DuplicatePatternException if the new pattern already exists in the store
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