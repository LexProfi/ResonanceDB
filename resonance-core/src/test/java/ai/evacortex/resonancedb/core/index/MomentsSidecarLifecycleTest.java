/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.engine.CompareOptions;
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.PhaseWeights;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lifecycle and crash/restart safety tests for ResonanceMoments sidecar.
 *
 * <p>Validates:
 * <ul>
 *   <li>Missing sidecar → fallback, no crash</li>
 *   <li>Corrupt sidecar → rejected, fallback</li>
 *   <li>Old IVF without sidecar → graceful degradation</li>
 *   <li>Sidecar wrong generation → compatibility check rejects</li>
 *   <li>Rebuild interruption → no stale data</li>
 *   <li>Index rebuild regenerates moments</li>
 *   <li>Insert/delete lifecycle → moments stay consistent after rebuild</li>
 * </ul>
 * </p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MomentsSidecarLifecycleTest {

    private static final int DIM = 64;
    private static final int N = 200;
    private static final long SEED = 42L;
    private static final int TOP_K = 5;

    private StoreRuntimeServices runtime;

    @BeforeAll
    void initRuntime() {
        runtime = StoreRuntimeServices.fromSystemProperties();
    }

    @AfterAll
    void closeRuntime() {
        if (runtime != null) runtime.close();
    }
    @Test
    @Order(1)
    @DisplayName("Missing moments sidecar → weighted query succeeds via fallback")
    void missingMomentsSidecar(@TempDir Path tmpDir) throws IOException {
        System.setProperty("resonance.index.enabled", "true");
        try {
            WavePatternStoreImpl store = createAndPopulate(tmpDir.resolve("missing"), N);
            try {
                store.forceIndexRebuild();

                Path momentsPath = tmpDir.resolve("missing/index/moments.rmom");
                Files.deleteIfExists(momentsPath);

                CompareOptions sparse = sparseOptions();
                List<ResonanceMatch> results = store.query(randomQuery(), TOP_K, sparse);
                assertNotNull(results);
                assertFalse(results.isEmpty(), "Query should return results even without moments");
            } finally {
                store.close();
            }
        } finally {
            System.clearProperty("resonance.index.enabled");
        }
    }

    @Test
    @Order(2)
    @DisplayName("Corrupt moments sidecar → rejected on load, fallback")
    void corruptMomentsSidecar(@TempDir Path tmpDir) throws IOException {
        System.setProperty("resonance.index.enabled", "true");
        try {
            WavePatternStoreImpl store = createAndPopulate(tmpDir.resolve("corrupt"), N);
            try {
                store.forceIndexRebuild();
                store.close();

                Path momentsPath = tmpDir.resolve("corrupt/index/moments.rmom");
                assertTrue(Files.exists(momentsPath), "Moments file should exist after build");

                byte[] data = Files.readAllBytes(momentsPath);
                for (int i = data.length / 3; i < data.length * 2 / 3; i++) {
                    data[i] = (byte) ~data[i];
                }
                Files.write(momentsPath, data);

                ResonanceMoments loaded = ResonanceMoments.load(momentsPath);
                assertNull(loaded, "Corrupt moments should return null on load");

                WavePatternStoreImpl store2 = new WavePatternStoreImpl(
                        tmpDir.resolve("corrupt"), DIM, runtime);
                try {
                    CompareOptions sparse = sparseOptions();
                    List<ResonanceMatch> results = store2.query(randomQuery(), TOP_K, sparse);
                    assertNotNull(results);
                } finally {
                    store2.close();
                }
            } finally {
                try { store.close(); } catch (Exception e) { /* already closed */ }
            }
        } finally {
            System.clearProperty("resonance.index.enabled");
        }
    }

    @Test
    @Order(3)
    @DisplayName("Old IVF without moments sidecar → graceful degradation")
    void oldIvfWithoutMoments(@TempDir Path tmpDir) throws IOException {
        System.setProperty("resonance.index.enabled", "true");
        try {
            WavePatternStoreImpl store = createAndPopulate(tmpDir.resolve("no-moments"), N);
            try {
                store.forceIndexRebuild();
                store.close();

                Path momentsPath = tmpDir.resolve("no-moments/index/moments.rmom");
                Files.deleteIfExists(momentsPath);

                WavePatternStoreImpl store2 = new WavePatternStoreImpl(
                        tmpDir.resolve("no-moments"), DIM, runtime);
                try {
                    List<ResonanceMatch> defaultResults = store2.query(randomQuery(), TOP_K);
                    assertFalse(defaultResults.isEmpty());

                    CompareOptions sparse = sparseOptions();
                    List<ResonanceMatch> weightedResults = store2.query(randomQuery(), TOP_K, sparse);
                    assertNotNull(weightedResults);
                    assertFalse(weightedResults.isEmpty());
                } finally {
                    store2.close();
                }
            } finally {
                try { store.close(); } catch (Exception e) { /* already closed */ }
            }
        } finally {
            System.clearProperty("resonance.index.enabled");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Moments from wrong generation → rejected by compatibility check")
    void wrongGenerationMoments(@TempDir Path tmpDir) throws IOException {
        int wrongK = 5;
        ResonanceMoments.Builder builder = new ResonanceMoments.Builder(wrongK, DIM);
        Random rng = new Random(SEED);
        for (int c = 0; c < wrongK; c++) {
            builder.addPattern(c, randomPattern(rng, DIM));
        }
        ResonanceMoments wrongMoments = builder.build();

        Path momentsPath = tmpDir.resolve("wrong-gen.rmom");
        wrongMoments.write(momentsPath);

        ResonanceMoments loaded = ResonanceMoments.load(momentsPath);
        assertNotNull(loaded, "File should load successfully");

        assertFalse(loaded.isCompatible(100, DIM),
                "Moments with K=5 should not be compatible with K=100");
        assertTrue(loaded.isCompatible(wrongK, DIM),
                "Moments should be compatible with matching K and D");

        assertFalse(loaded.isCompatible(wrongK, DIM * 2),
                "Moments should reject mismatched dimension");
    }

    @Test
    @Order(5)
    @DisplayName("Index rebuild regenerates fresh moments consistent with postings")
    void indexRebuildRegeneratesMoments(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        try {
            WavePatternStoreImpl store = createAndPopulate(tmpDir.resolve("rebuild"), N);
            try {
                store.forceIndexRebuild();

                Path momentsPath = tmpDir.resolve("rebuild/index/moments.rmom");
                assertTrue(Files.exists(momentsPath), "Moments should exist after first build");

                ResonanceMoments moments1 = ResonanceMoments.load(momentsPath);
                assertNotNull(moments1, "Moments should load after first build");

                Random rng = new Random(SEED + 999);
                for (int i = 0; i < 50; i++) {
                    try { store.insert(randomPattern(rng, DIM), Map.of()); }
                    catch (Exception e) { /* dup */ }
                }

                store.forceIndexRebuild();

                ResonanceMoments moments2 = ResonanceMoments.load(momentsPath);
                assertNotNull(moments2, "Moments should exist after second build");

                assertTrue(moments2.centroidCount() > 0);
                assertEquals(DIM, moments2.dimension());

            } finally {
                store.close();
            }
        } finally {
            System.clearProperty("resonance.index.enabled");
        }
    }

    @Test
    @Order(6)
    @DisplayName("Delete patterns → moments refreshed on rebuild → no stale stats")
    void deleteAndRebuild(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        try {
            Path storePath = tmpDir.resolve("delete-lifecycle");
            WavePatternStoreImpl store = new WavePatternStoreImpl(storePath, DIM, runtime);
            try {
                Random rng = new Random(SEED);
                List<String> ids = new ArrayList<>();
                for (int i = 0; i < N; i++) {
                    try {
                        ids.add(store.insert(randomPattern(rng, DIM), Map.of()));
                    } catch (Exception e) { /* dup */ }
                }
                store.forceIndexRebuild();

                Path momentsPath = storePath.resolve("index/moments.rmom");
                ResonanceMoments beforeDelete = ResonanceMoments.load(momentsPath);
                assertNotNull(beforeDelete);

                int totalBefore = 0;
                for (int c = 0; c < beforeDelete.centroidCount(); c++) {
                    totalBefore += beforeDelete.count(c);
                }

                int deleteCount = ids.size() / 2;
                for (int i = 0; i < deleteCount; i++) {
                    store.delete(ids.get(i));
                }

                store.forceIndexRebuild();

                ResonanceMoments afterDelete = ResonanceMoments.load(momentsPath);
                assertNotNull(afterDelete);

                int totalAfter = 0;
                for (int c = 0; c < afterDelete.centroidCount(); c++) {
                    totalAfter += afterDelete.count(c);
                }

                assertTrue(totalAfter < totalBefore,
                        "Moments should reflect fewer patterns after deletion + rebuild");

                CompareOptions sparse = sparseOptions();
                List<ResonanceMatch> results = store.query(randomQuery(), TOP_K, sparse);
                assertNotNull(results);

                Set<String> deletedIds = new HashSet<>(ids.subList(0, deleteCount));
                for (ResonanceMatch r : results) {
                    assertFalse(deletedIds.contains(r.id()),
                            "Deleted pattern should not appear in results");
                }

            } finally {
                store.close();
            }
        } finally {
            System.clearProperty("resonance.index.enabled");
        }
    }

    @Test
    @Order(7)
    @DisplayName("Moments write → load roundtrip preserves all statistics")
    void momentsPersistenceRoundtrip(@TempDir Path tmpDir) throws IOException {
        int K = 4;
        ResonanceMoments.Builder builder = new ResonanceMoments.Builder(K, DIM);
        Random rng = new Random(SEED);

        for (int c = 0; c < K; c++) {
            int patternsPerCentroid = 10 + c * 5;
            for (int i = 0; i < patternsPerCentroid; i++) {
                builder.addPattern(c, randomPattern(rng, DIM));
            }
        }

        ResonanceMoments original = builder.build();
        Path path = tmpDir.resolve("test.rmom");
        original.write(path);

        ResonanceMoments loaded = ResonanceMoments.load(path);
        assertNotNull(loaded);
        assertEquals(K, loaded.centroidCount());
        assertEquals(DIM, loaded.dimension());

        for (int c = 0; c < K; c++) {
            assertEquals(original.count(c), loaded.count(c));
            assertEquals(original.meanEnergy(c), loaded.meanEnergy(c), 1e-5);
            assertArrayEquals(original.meanAmplitude(c), loaded.meanAmplitude(c), 1e-5f);
            assertArrayEquals(original.meanRealProj(c), loaded.meanRealProj(c), 1e-5f);
            assertArrayEquals(original.meanImagProj(c), loaded.meanImagProj(c), 1e-5f);
        }
    }

    @Test
    @Order(8)
    @DisplayName("Truncated moments file → returns null (no crash)")
    void truncatedMomentsFile(@TempDir Path tmpDir) throws IOException {
        int K = 3;
        ResonanceMoments.Builder builder = new ResonanceMoments.Builder(K, DIM);
        Random rng = new Random(SEED);
        for (int c = 0; c < K; c++) {
            builder.addPattern(c, randomPattern(rng, DIM));
        }
        ResonanceMoments original = builder.build();
        Path path = tmpDir.resolve("truncated.rmom");
        original.write(path);

        byte[] data = Files.readAllBytes(path);
        Files.write(path, Arrays.copyOf(data, data.length / 2));

        ResonanceMoments loaded = ResonanceMoments.load(path);
        assertNull(loaded, "Truncated file should return null");
    }

    @Test
    @Order(9)
    @DisplayName("Empty moments file → returns null (no crash)")
    void emptyMomentsFile(@TempDir Path tmpDir) throws IOException {
        Path path = tmpDir.resolve("empty.rmom");
        Files.write(path, new byte[0]);
        assertNull(ResonanceMoments.load(path));
    }

    @Test
    @Order(10)
    @DisplayName("File with wrong magic → returns null")
    void wrongMagicMomentsFile(@TempDir Path tmpDir) throws IOException {
        Path path = tmpDir.resolve("wrong-magic.rmom");
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(path.toFile()))) {
            out.writeInt(0xDEADBEEF);
            out.writeInt(1);
            out.writeInt(2);
            out.writeInt(DIM);
        }
        assertNull(ResonanceMoments.load(path));
    }

    @Test
    @Order(11)
    @DisplayName("Weighted query gives correct results after store restart")
    void weightedQueryAfterRestart(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        try {
            Path storePath = tmpDir.resolve("restart");
            WavePattern query = randomQuery();
            CompareOptions sparse = sparseOptions();

            List<ResonanceMatch> resultsBefore;
            {
                WavePatternStoreImpl store = createAndPopulate(storePath, N);
                store.forceIndexRebuild();
                resultsBefore = store.query(query, TOP_K, sparse);
                store.close();
            }

            List<ResonanceMatch> resultsAfter;
            {
                WavePatternStoreImpl store = new WavePatternStoreImpl(storePath, DIM, runtime);
                resultsAfter = store.query(query, TOP_K, sparse);
                store.close();
            }

            assertEquals(resultsBefore.size(), resultsAfter.size(),
                    "Same number of results before and after restart");

            for (int i = 0; i < resultsBefore.size(); i++) {
                assertEquals(resultsBefore.get(i).id(), resultsAfter.get(i).id(),
                        "Result " + i + " should be identical after restart");
                assertEquals(resultsBefore.get(i).energy(), resultsAfter.get(i).energy(), 1e-5f,
                        "Score for result " + i + " should be identical after restart");
            }
        } finally {
            System.clearProperty("resonance.index.enabled");
        }
    }

    @Test
    @Order(12)
    @DisplayName("Moments memory scales as O(centroids × dim), not O(patterns × dim)")
    void momentsMemoryScaling(@TempDir Path tmpDir) throws IOException {
        int K = 8;
        int D = 128;

        long[] sizes = new long[3];
        int[] patternCounts = {100, 1000, 5000};

        for (int trial = 0; trial < patternCounts.length; trial++) {
            ResonanceMoments.Builder builder = new ResonanceMoments.Builder(K, D);
            Random rng = new Random(SEED + trial);
            for (int i = 0; i < patternCounts[trial]; i++) {
                builder.addPattern(rng.nextInt(K), randomPattern(rng, D));
            }
            ResonanceMoments moments = builder.build();
            Path path = tmpDir.resolve("scale-" + trial + ".rmom");
            moments.write(path);
            sizes[trial] = Files.size(path);
        }

        for (int i = 1; i < sizes.length; i++) {
            assertEquals(sizes[0], sizes[i],
                    "Moments file size should be identical for same K×D regardless of pattern count: " +
                    "N=" + patternCounts[0] + " → " + sizes[0] + " bytes, " +
                    "N=" + patternCounts[i] + " → " + sizes[i] + " bytes");
        }

        long expectedSize = 16 + (long) K * (4 + 4 + 3L * D * 4) + 4;
        assertEquals(expectedSize, sizes[0],
                "File size should match expected O(K×D) formula");
    }

    private WavePatternStoreImpl createAndPopulate(Path dir, int count) {
        WavePatternStoreImpl store = new WavePatternStoreImpl(dir, DIM, runtime);
        Random rng = new Random(SEED);
        for (int i = 0; i < count; i++) {
            try { store.insert(randomPattern(rng, DIM), Map.of()); }
            catch (Exception e) { /* dup */ }
        }
        return store;
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

    private WavePattern randomQuery() {
        return randomPattern(new Random(SEED + 12345), DIM);
    }

    private CompareOptions sparseOptions() {
        double[] w = new double[DIM];
        Random rng = new Random(MASK_SEED);
        int active = DIM / 10;
        List<Integer> indices = new ArrayList<>(DIM);
        for (int i = 0; i < DIM; i++) indices.add(i);
        Collections.shuffle(indices, rng);
        for (int i = 0; i < active; i++) w[indices.get(i)] = 1.0;
        return CompareOptions.withPhaseWeights(new PhaseWeights(w));
    }

    private static final long MASK_SEED = 577215L;
}
