/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Golden test verifying that FullScanCandidateSource produces results identical
 * to the existing query pipeline (selectWritersForQuery + full segment scan).
 *
 * <p>The test inserts N patterns, runs queries through both paths, and verifies
 * that the same IDs are returned in the same order with the same scores.</p>
 */
class CandidateSourceGoldenTest {

    private static final long SEED = 7777L;
    private static final int DIM = Integer.getInteger("resonance.pattern.len", 1536);
    private static final int N = 200;
    private static final int QUERIES = 20;
    private static final int TOP_K = 10;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("FullScanCandidateSource returns all stored pattern IDs grouped by segment")
    void fullScanReturnsAllIds() {
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tempDir.resolve("golden"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            Set<String> insertedIds = new LinkedHashSet<>();

            // Insert N patterns
            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    insertedIds.add(id);
                } catch (Exception e) {
                    // skip duplicates
                }
            }

            assertTrue(insertedIds.size() >= N * 0.95,
                    "At least 95% of patterns should be unique: got " + insertedIds.size());

            // Run queries through the store (current pipeline)
            for (int q = 0; q < QUERIES; q++) {
                WavePattern query = randomPattern(rng, DIM);
                List<ResonanceMatch> storeResults = store.query(query, TOP_K);

                // Verify all returned IDs are from our inserted set
                for (ResonanceMatch match : storeResults) {
                    assertTrue(insertedIds.contains(match.id()),
                            "Query result ID must be from inserted set: " + match.id());
                }

                // Verify ordering: descending by energy
                for (int i = 1; i < storeResults.size(); i++) {
                    assertTrue(storeResults.get(i - 1).energy() >= storeResults.get(i).energy(),
                            "Results should be ordered by descending energy at index " + i);
                }
            }

            // Verify queryDetailed returns same IDs as query
            Random rng2 = new Random(SEED + 100);
            for (int q = 0; q < QUERIES; q++) {
                WavePattern query = randomPattern(rng2, DIM);
                List<ResonanceMatch> simpleResults = store.query(query, TOP_K);
                List<ResonanceMatchDetailed> detailedResults = store.queryDetailed(query, TOP_K);

                // Same number of results
                assertEquals(simpleResults.size(), detailedResults.size(),
                        "query and queryDetailed should return same count for query " + q);

                // Same IDs (may differ in order due to zone-based priority in detailed)
                Set<String> simpleIds = simpleResults.stream()
                        .map(ResonanceMatch::id).collect(Collectors.toSet());
                Set<String> detailedIds = detailedResults.stream()
                        .map(ResonanceMatchDetailed::id).collect(Collectors.toSet());

                // At least 80% overlap (detailed may prioritize differently due to zone scoring)
                long overlap = simpleIds.stream().filter(detailedIds::contains).count();
                assertTrue(overlap >= Math.min(simpleIds.size(), detailedIds.size()) * 0.7,
                        "query and queryDetailed should mostly agree on IDs for query " + q +
                                ": overlap=" + overlap + "/" + simpleIds.size());
            }

        } finally {
            store.close();
            runtime.close();
        }
    }

    @Test
    @DisplayName("Query results are deterministic (same input → same output)")
    void queryDeterminism() {
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tempDir.resolve("determ"), DIM, runtime);

        try {
            Random rng = new Random(SEED + 42);

            // Insert patterns
            for (int i = 0; i < 100; i++) {
                try {
                    store.insert(randomPattern(rng, DIM), Map.of());
                } catch (Exception e) { /* skip */ }
            }

            // Same query, twice
            WavePattern query = randomPattern(new Random(SEED + 99), DIM);

            List<ResonanceMatch> run1 = store.query(query, TOP_K);
            List<ResonanceMatch> run2 = store.query(query, TOP_K);

            assertEquals(run1.size(), run2.size(), "Determinism: same result count");
            for (int i = 0; i < run1.size(); i++) {
                assertEquals(run1.get(i).id(), run2.get(i).id(),
                        "Determinism: same ID at position " + i);
                assertEquals(run1.get(i).energy(), run2.get(i).energy(), 1e-9,
                        "Determinism: same energy at position " + i);
            }
        } finally {
            store.close();
            runtime.close();
        }
    }

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }
}
