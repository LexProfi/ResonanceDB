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
import ai.evacortex.resonancedb.core.storage.WavePattern;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Vamana graph construction, search, and persistence.
 */
class VamanaTest {

    private static final long SEED = 42L;
    private static final int DIM = 32;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("VamanaBuilder: constructs graph with correct structure")
    void buildGraph() {
        int n = 100;
        WavePattern[] patterns = randomPatterns(n, DIM, SEED);
        String[] ids = makeIds(n);

        VamanaGraph graph = VamanaBuilder.build(ids, patterns, 16, 64, 1.2, SEED);

        assertEquals(n, graph.nodeCount());
        assertEquals(16, graph.maxDegree());
        assertTrue(graph.medoid() >= 0 && graph.medoid() < n);

        int nodesWithNeighbors = 0;
        for (int[] nbrs : graph.neighbors()) {
            if (nbrs.length > 0) nodesWithNeighbors++;
            assertTrue(nbrs.length <= 16, "Degree must not exceed R");
        }
        assertTrue(nodesWithNeighbors >= n * 0.9, "At least 90% of nodes should have neighbors");
    }

    @Test
    @DisplayName("VamanaBuilder: deterministic (same seed → same graph)")
    void buildDeterministic() {
        WavePattern[] patterns = randomPatterns(50, DIM, SEED);
        String[] ids = makeIds(50);

        VamanaGraph g1 = VamanaBuilder.build(ids, patterns, SEED);
        VamanaGraph g2 = VamanaBuilder.build(ids, patterns, SEED);

        assertEquals(g1.medoid(), g2.medoid());
        for (int i = 0; i < g1.nodeCount(); i++) {
            assertArrayEquals(g1.neighbors()[i], g2.neighbors()[i],
                    "Neighbors of node " + i + " should match");
        }
    }

    @Test
    @DisplayName("Vamana search: returns correct topK results with high recall")
    void searchRecall() {
        int n = 300;
        int topK = 10;
        int efSearch = 64;
        ResonanceKernel kernel = new JavaKernel();

        WavePattern[] patterns = randomPatterns(n, DIM, SEED);
        String[] ids = makeIds(n);

        VamanaGraph graph = VamanaBuilder.build(ids, patterns, 24, 64, 1.2, SEED);

        int totalHits = 0;
        int totalExpected = 0;
        int queries = 50;

        Random qRng = new Random(SEED + 777);
        for (int q = 0; q < queries; q++) {
            WavePattern query = randomPattern(qRng, DIM);

            float[] scores = new float[n];
            for (int i = 0; i < n; i++) {
                scores[i] = kernel.compare(query, patterns[i]);
            }
            Integer[] sorted = new Integer[n];
            for (int i = 0; i < n; i++) sorted[i] = i;
            Arrays.sort(sorted, (a, b) -> Float.compare(scores[b], scores[a]));

            Set<String> trueTopK = new HashSet<>();
            for (int i = 0; i < topK; i++) {
                trueTopK.add(ids[sorted[i]]);
            }

            List<String> vamanaResults = graph.search(query, patterns, efSearch, topK);

            for (String id : vamanaResults) {
                if (trueTopK.contains(id)) totalHits++;
            }
            totalExpected += topK;
        }

        double recall = (double) totalHits / totalExpected;
        System.out.println("Vamana recall@" + topK + " (efSearch=" + efSearch + "): " +
                String.format("%.3f", recall) + " (" + totalHits + "/" + totalExpected + ")");

        assertTrue(recall >= 0.85,
                "Vamana recall@" + topK + " should be >= 0.85, got " + recall);
    }

    @Test
    @DisplayName("Vamana search: small partition falls back to brute force")
    void searchSmallPartition() {
        int n = 10;
        WavePattern[] patterns = randomPatterns(n, DIM, SEED);
        String[] ids = makeIds(n);

        VamanaGraph graph = VamanaBuilder.build(ids, patterns, SEED);
        WavePattern query = randomPattern(new Random(SEED + 1), DIM);

        List<String> results = graph.search(query, patterns, 100, 5);
        assertEquals(5, results.size());
        for (String id : results) {
            assertTrue(id.startsWith("id-"));
        }
    }

    @Test
    @DisplayName("VamanaPersistence: write then read round-trip")
    void persistenceRoundTrip() throws IOException {
        WavePattern[] patterns = randomPatterns(50, DIM, SEED);
        String[] ids = makeIds(50);

        VamanaGraph original = VamanaBuilder.build(ids, patterns, 16, 64, 1.2, SEED);
        Path path = tempDir.resolve("test.vamana");

        VamanaPersistence.write(path, original);
        VamanaGraph loaded = VamanaPersistence.read(path);

        assertNotNull(loaded);
        assertEquals(original.nodeCount(), loaded.nodeCount());
        assertEquals(original.maxDegree(), loaded.maxDegree());
        assertEquals(original.medoid(), loaded.medoid());

        for (int i = 0; i < original.nodeCount(); i++) {
            assertEquals(original.patternIds()[i], loaded.patternIds()[i]);
            assertArrayEquals(original.neighbors()[i], loaded.neighbors()[i]);
        }
    }

    @Test
    @DisplayName("VamanaPersistence: corrupted file returns null")
    void persistenceCorrupted() throws IOException {
        Path path = tempDir.resolve("corrupted.vamana");
        java.nio.file.Files.write(path, new byte[]{1, 2, 3, 4});
        assertNull(VamanaPersistence.read(path));
    }

    @Test
    @DisplayName("VamanaPersistence: missing file returns null")
    void persistenceMissing() {
        assertNull(VamanaPersistence.read(tempDir.resolve("nope.vamana")));
    }

    @Test
    @DisplayName("VamanaBuilder: single node graph")
    void singleNode() {
        WavePattern[] patterns = randomPatterns(1, DIM, SEED);
        VamanaGraph graph = VamanaBuilder.build(new String[]{"id-0"}, patterns, SEED);

        assertEquals(1, graph.nodeCount());
        assertEquals(0, graph.neighbors()[0].length);

        List<String> results = graph.search(patterns[0], patterns, 64, 1);
        assertEquals(1, results.size());
        assertEquals("id-0", results.get(0));
    }

    @Test
    @DisplayName("VamanaBuilder: empty graph")
    void emptyGraph() {
        VamanaGraph graph = VamanaBuilder.build(new String[0], new WavePattern[0], SEED);
        assertEquals(0, graph.nodeCount());
        assertTrue(graph.search(randomPattern(new Random(1), DIM), new WavePattern[0], 64, 10).isEmpty());
    }

    private static WavePattern[] randomPatterns(int n, int dim, long seed) {
        Random rng = new Random(seed);
        WavePattern[] patterns = new WavePattern[n];
        for (int i = 0; i < n; i++) {
            patterns[i] = randomPattern(rng, dim);
        }
        return patterns;
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

    private static String[] makeIds(int n) {
        String[] ids = new String[n];
        for (int i = 0; i < n; i++) ids[i] = "id-" + i;
        return ids;
    }
}
