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
import ai.evacortex.resonancedb.core.math.UnfoldedMath;
import ai.evacortex.resonancedb.core.storage.WavePattern;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for IVF-Flat index components: SphericalKMeans, CentroidIndex,
 * CentroidPersistence, DeltaIndex.
 */
class IvfIndexTest {

    private static final long SEED = 42L;
    private static final int DIM = 32; // small dim for speed
    private static final int UNFOLDED_DIM = DIM * 2;

    @TempDir
    Path tempDir;

    // ─── SphericalKMeans ─────────────────────────────────────────────────────

    @Test
    @DisplayName("k-means: K centroids produced, all L2-normalized")
    void kmeansProducesCentroids() {
        int n = 500;
        int k = 16;
        double[][] data = randomNormalizedVectors(n, UNFOLDED_DIM, SEED);

        double[][] centroids = SphericalKMeans.fit(data, k, UNFOLDED_DIM, SEED);

        assertEquals(k, centroids.length);
        for (double[] c : centroids) {
            assertEquals(UNFOLDED_DIM, c.length);
            double norm = Math.sqrt(UnfoldedMath.dotUnfolded(c, c));
            assertEquals(1.0, norm, 1e-6, "Centroid must be L2-normalized");
        }
    }

    @Test
    @DisplayName("k-means: deterministic (same seed → same centroids)")
    void kmeansDeterministic() {
        double[][] data = randomNormalizedVectors(200, UNFOLDED_DIM, SEED);
        double[][] c1 = SphericalKMeans.fit(data, 8, UNFOLDED_DIM, 123L);
        double[][] c2 = SphericalKMeans.fit(data, 8, UNFOLDED_DIM, 123L);

        for (int i = 0; i < c1.length; i++) {
            assertArrayEquals(c1[i], c2[i], 1e-12, "Centroid " + i + " must match");
        }
    }

    @Test
    @DisplayName("k-means: recommended K formula")
    void recommendedK() {
        assertEquals(16, SphericalKMeans.recommendedK(100));
        assertEquals(16, SphericalKMeans.recommendedK(256));
        assertEquals(100, SphericalKMeans.recommendedK(10000));
        assertEquals(3163, SphericalKMeans.recommendedK(10_000_000));
    }

    @Test
    @DisplayName("k-means: K clamped to N when K > N")
    void kmeansKGreaterThanN() {
        double[][] data = randomNormalizedVectors(5, UNFOLDED_DIM, SEED);
        double[][] centroids = SphericalKMeans.fit(data, 100, UNFOLDED_DIM, SEED);
        assertEquals(5, centroids.length, "K clamped to N");
    }

    // ─── CentroidIndex ───────────────────────────────────────────────────────

    @Test
    @DisplayName("CentroidIndex: assigns patterns to nearest centroid(s)")
    void centroidIndexAssignment() {
        double[][] centroids = randomNormalizedVectors(4, UNFOLDED_DIM, SEED);
        CentroidIndex index = new CentroidIndex(centroids, UNFOLDED_DIM, 0.15);

        // Assign 100 patterns
        Random rng = new Random(SEED + 1);
        for (int i = 0; i < 100; i++) {
            WavePattern p = randomPattern(rng, DIM);
            index.assignPattern("id-" + i, p);
        }

        assertEquals(100, index.uniquePatternCount());
        assertTrue(index.totalPostings() >= 100, "With boundary replication, postings >= unique patterns");
    }

    @Test
    @DisplayName("CentroidIndex: queryCandidates returns subset with nProbe < K")
    void centroidIndexQuerySubset() {
        int k = 8;
        double[][] data = randomNormalizedVectors(200, UNFOLDED_DIM, SEED);
        double[][] centroids = SphericalKMeans.fit(data, k, UNFOLDED_DIM, SEED);
        CentroidIndex index = new CentroidIndex(centroids, UNFOLDED_DIM, 0.15);

        Random rng = new Random(SEED + 2);
        for (int i = 0; i < 200; i++) {
            WavePattern p = randomPattern(rng, DIM);
            index.assignPattern("id-" + i, p);
        }

        WavePattern query = randomPattern(new Random(SEED + 99), DIM);

        Set<String> candidates1 = index.queryCandidates(query, 1);
        Set<String> candidates4 = index.queryCandidates(query, 4);
        Set<String> candidatesAll = index.queryCandidates(query, k);

        assertTrue(candidates1.size() <= candidates4.size(),
                "More probes → more candidates");
        assertTrue(candidates4.size() <= candidatesAll.size(),
                "More probes → more candidates");
        assertEquals(200, candidatesAll.size(),
                "Probing all centroids should return all patterns");
    }

    @Test
    @DisplayName("CentroidIndex: removePattern removes from all postings")
    void centroidIndexRemove() {
        double[][] centroids = randomNormalizedVectors(4, UNFOLDED_DIM, SEED);
        CentroidIndex index = new CentroidIndex(centroids, UNFOLDED_DIM, 0.15);

        WavePattern p = randomPattern(new Random(SEED), DIM);
        index.assignPattern("target", p);
        assertEquals(1, index.uniquePatternCount());

        index.removePattern("target");
        assertEquals(0, index.uniquePatternCount());
    }

    // ─── CentroidPersistence ─────────────────────────────────────────────────

    @Test
    @DisplayName("CentroidPersistence: write then read produces identical centroids")
    void persistenceRoundTrip() throws IOException {
        double[][] centroids = randomNormalizedVectors(8, UNFOLDED_DIM, SEED);
        Path path = tempDir.resolve("test-centroids.bin");

        CentroidPersistence.write(path, centroids, UNFOLDED_DIM, 0.15);
        CentroidIndex loaded = CentroidPersistence.read(path);

        assertNotNull(loaded);
        assertEquals(8, loaded.size());
        assertEquals(UNFOLDED_DIM, loaded.dim());
        assertEquals(0.15, loaded.lambda(), 1e-12);

        for (int c = 0; c < centroids.length; c++) {
            assertArrayEquals(centroids[c], loaded.centroids()[c], 1e-12,
                    "Centroid " + c + " round-trip mismatch");
        }
    }

    @Test
    @DisplayName("CentroidPersistence: corrupted file returns null")
    void persistenceCorrupted() throws IOException {
        Path path = tempDir.resolve("corrupted.bin");
        java.nio.file.Files.write(path, new byte[]{1, 2, 3, 4, 5});

        assertNull(CentroidPersistence.read(path));
    }

    @Test
    @DisplayName("CentroidPersistence: missing file returns null")
    void persistenceMissing() {
        assertNull(CentroidPersistence.read(tempDir.resolve("nonexistent.bin")));
    }

    // ─── DeltaIndex ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("DeltaIndex: add and retrieve")
    void deltaAddAndRetrieve() {
        DeltaIndex delta = new DeltaIndex();
        delta.add("id-1", "seg-0");
        delta.add("id-2", "seg-0");
        delta.add("id-3", "seg-1");

        assertEquals(3, delta.size());

        Set<String> ids = delta.allIds();
        assertEquals(Set.of("id-1", "id-2", "id-3"), ids);

        Map<String, Collection<String>> bySegment = delta.candidatesBySegment();
        assertEquals(2, bySegment.size());
        assertTrue(bySegment.get("seg-0").contains("id-1"));
        assertTrue(bySegment.get("seg-1").contains("id-3"));
    }

    @Test
    @DisplayName("DeltaIndex: deleted patterns excluded")
    void deltaDeletedExcluded() {
        DeltaIndex delta = new DeltaIndex();
        delta.add("id-1", "seg-0");
        delta.add("id-2", "seg-0");
        delta.markDeleted("id-1");

        Set<String> ids = delta.allIds();
        assertEquals(Set.of("id-2"), ids);
        assertTrue(delta.isDeleted("id-1"));
    }

    @Test
    @DisplayName("DeltaIndex: drain clears buffer")
    void deltaDrain() {
        DeltaIndex delta = new DeltaIndex();
        delta.add("id-1", "seg-0");
        delta.add("id-2", "seg-1");

        List<DeltaIndex.DeltaEntry> drained = delta.drainAll();
        assertEquals(2, drained.size());
        assertEquals(0, delta.size());
        assertTrue(delta.allIds().isEmpty());
    }

    @Test
    @DisplayName("DeltaIndex: threshold checks")
    void deltaThreshold() {
        DeltaIndex delta = new DeltaIndex();
        for (int i = 0; i < 100; i++) {
            delta.add("id-" + i, "seg-0");
        }

        assertTrue(delta.exceedsThreshold(50));
        assertFalse(delta.exceedsThreshold(200));
        assertTrue(delta.exceedsFraction(1000, 0.05)); // 100 > 50
        assertFalse(delta.exceedsFraction(10000, 0.05)); // 100 < 500
    }

    // ─── IVF Recall Test ─────────────────────────────────────────────────────

    @Test
    @DisplayName("IVF recall: ANN candidates contain true top-K with high probability")
    void ivfRecall() {
        int n = 500;
        int k = 16;
        int nProbe = 4;
        int topK = 10;
        ResonanceKernel kernel = new JavaKernel();
        Random rng = new Random(SEED);

        // Generate patterns
        WavePattern[] patterns = new WavePattern[n];
        for (int i = 0; i < n; i++) {
            patterns[i] = randomPattern(rng, DIM);
        }

        // Build IVF index
        double[][] data = new double[n][];
        for (int i = 0; i < n; i++) {
            data[i] = UnfoldedMath.unfold(patterns[i]);
            UnfoldedMath.l2Normalize(data[i]);
        }

        double[][] centroids = SphericalKMeans.fit(data, k, UNFOLDED_DIM, SEED);
        CentroidIndex index = new CentroidIndex(centroids, UNFOLDED_DIM, 0.15);
        for (int i = 0; i < n; i++) {
            index.assignNormalized("id-" + i, data[i]);
        }

        // Run queries and measure recall
        int totalHits = 0;
        int totalExpected = 0;
        int queries = 50;

        Random qRng = new Random(SEED + 777);
        for (int q = 0; q < queries; q++) {
            WavePattern query = randomPattern(qRng, DIM);

            // Exact top-K by kernel score
            float[] scores = new float[n];
            for (int i = 0; i < n; i++) {
                scores[i] = kernel.compare(query, patterns[i]);
            }
            Integer[] sortedIndices = new Integer[n];
            for (int i = 0; i < n; i++) sortedIndices[i] = i;
            Arrays.sort(sortedIndices, (a, b) -> Float.compare(scores[b], scores[a]));

            Set<String> trueTopK = new HashSet<>();
            for (int i = 0; i < topK; i++) {
                trueTopK.add("id-" + sortedIndices[i]);
            }

            // ANN candidates
            Set<String> annCandidates = index.queryCandidates(query, nProbe);

            // Count hits
            for (String id : trueTopK) {
                if (annCandidates.contains(id)) {
                    totalHits++;
                }
            }
            totalExpected += topK;
        }

        double recall = (double) totalHits / totalExpected;
        System.out.println("IVF recall@" + topK + " with nProbe=" + nProbe +
                ": " + String.format("%.3f", recall) +
                " (" + totalHits + "/" + totalExpected + ")");

        assertTrue(recall >= 0.80,
                "IVF recall@" + topK + " should be >= 0.80, got " + recall);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static double[][] randomNormalizedVectors(int n, int dim, long seed) {
        Random rng = new Random(seed);
        double[][] data = new double[n][dim];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dim; d++) {
                data[i][d] = rng.nextGaussian();
            }
            UnfoldedMath.l2Normalize(data[i]);
        }
        return data;
    }
}
