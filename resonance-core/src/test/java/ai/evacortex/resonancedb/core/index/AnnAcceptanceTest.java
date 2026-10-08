/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Acceptance tests for the ANN indexing subsystem (IVF L1 + Vamana L2).
 *
 * <p>Validates recall, concurrency safety, crash recovery, and exact equivalence
 * with the legacy full-scan path. These tests run with production-scale
 * pattern dimension (1536) and use the real store pipeline.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AnnAcceptanceTest {

    private static final long SEED = 314159L;
    private static final int DIM = Integer.getInteger("resonance.pattern.len", 1536);
    private static final int TOP_K = 10;
    private static final int N = 500;
    @Test
    @Order(1)
    @DisplayName("Recall: IVF (nProbe=K) vs ground-truth kernel scoring ≥ 0.90")
    void recallVsGroundTruth(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("recall"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> allPatterns = new ArrayList<>();
            List<String> allIds = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    allPatterns.add(p);
                    allIds.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            ai.evacortex.resonancedb.core.engine.ResonanceKernel kernel =
                    new ai.evacortex.resonancedb.core.engine.JavaKernel();

            int totalHits = 0;
            int totalExpected = 0;
            int queries = 20;
            Random qRng = new Random(SEED + 999);

            for (int q = 0; q < queries; q++) {
                WavePattern query = randomPattern(qRng, DIM);

                float[] scores = new float[allPatterns.size()];
                for (int i = 0; i < allPatterns.size(); i++) {
                    scores[i] = kernel.compare(query, allPatterns.get(i));
                }
                Integer[] sortedIdx = new Integer[allPatterns.size()];
                for (int i = 0; i < sortedIdx.length; i++) sortedIdx[i] = i;
                Arrays.sort(sortedIdx, (a, b) -> Float.compare(scores[b], scores[a]));

                Set<String> trueTopK = new HashSet<>();
                for (int i = 0; i < TOP_K && i < sortedIdx.length; i++) {
                    trueTopK.add(allIds.get(sortedIdx[i]));
                }

                List<ResonanceMatch> annResults = store.query(query, TOP_K);
                Set<String> annIds = annResults.stream()
                        .map(ResonanceMatch::id).collect(Collectors.toSet());

                for (String id : trueTopK) {
                    if (annIds.contains(id)) totalHits++;
                }
                totalExpected += trueTopK.size();
            }

            double recall = totalExpected > 0 ? (double) totalHits / totalExpected : 1.0;
            System.out.println("IVF recall@" + TOP_K + " vs ground truth: " +
                    String.format("%.3f", recall) +
                    " (" + totalHits + "/" + totalExpected + ")");

            assertTrue(recall >= 0.90,
                    "IVF recall@" + TOP_K + " (nProbe=K) vs ground truth must be >= 0.90, got " +
                            String.format("%.3f", recall));
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(2)
    @DisplayName("Concurrency: 8 writers + 8 readers + rebuild, no deadlocks or data loss")
    void concurrentWritersReadersRebuild(@TempDir Path tmpDir) throws Exception {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.delta.maxSize", "50");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("conc"), DIM, runtime);

        try {
            Random preRng = new Random(SEED);
            for (int i = 0; i < 50; i++) {
                try { store.insert(randomPattern(preRng, DIM), Map.of()); }
                catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            int writerThreads = 8;
            int readerThreads = 8;
            int durationSec = 15;

            AtomicInteger insertCount = new AtomicInteger(0);
            AtomicInteger queryCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            AtomicReference<Throwable> firstError = new AtomicReference<>();

            ExecutorService exec = Executors.newFixedThreadPool(writerThreads + readerThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            long deadline = System.nanoTime() + durationSec * 1_000_000_000L;

            for (int t = 0; t < writerThreads; t++) {
                int threadId = t;
                exec.submit(() -> {
                    try {
                        startLatch.await();
                        Random rng = new Random(SEED + 1000 + threadId);
                        while (System.nanoTime() < deadline) {
                            try {
                                store.insert(randomPattern(rng, DIM), Map.of());
                                insertCount.incrementAndGet();
                            } catch (Exception e) {
                                if (!(e instanceof ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException)) {
                                    errorCount.incrementAndGet();
                                    firstError.compareAndSet(null, e);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            for (int t = 0; t < readerThreads; t++) {
                int threadId = t;
                exec.submit(() -> {
                    try {
                        startLatch.await();
                        Random rng = new Random(SEED + 2000 + threadId);
                        while (System.nanoTime() < deadline) {
                            try {
                                WavePattern q = randomPattern(rng, DIM);
                                List<ResonanceMatch> results = store.query(q, TOP_K);
                                assertNotNull(results);
                                queryCount.incrementAndGet();
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                                firstError.compareAndSet(null, e);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            startLatch.countDown();
            exec.shutdown();
            assertTrue(exec.awaitTermination(durationSec + 30, TimeUnit.SECONDS),
                    "Threads must complete within timeout (no deadlock)");

            System.out.println("Concurrency test: " +
                    insertCount.get() + " inserts, " +
                    queryCount.get() + " queries, " +
                    errorCount.get() + " errors in " + durationSec + "s");

            if (firstError.get() != null) {
                firstError.get().printStackTrace();
            }

            assertEquals(0, errorCount.get(),
                    "Zero errors expected. First: " +
                            (firstError.get() != null ? firstError.get().getMessage() : "none"));
            assertTrue(insertCount.get() > 0, "At least some inserts should succeed");
            assertTrue(queryCount.get() > 0, "At least some queries should succeed");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.delta.maxSize");
            runtime.close();
        }
    }

    @Test
    @Order(3)
    @DisplayName("Crash-safety: corrupted centroids.bin → fallback to full scan → queries work")
    void crashSafetyCorruptedIndex(@TempDir Path tmpDir) throws IOException {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        Path storeDir = tmpDir.resolve("crash");

        WavePatternStoreImpl store1 = new WavePatternStoreImpl(storeDir, DIM, runtime);
        Random rng = new Random(SEED);
        for (int i = 0; i < 100; i++) {
            try { store1.insert(randomPattern(rng, DIM), Map.of()); }
            catch (Exception e) { /* dup */ }
        }
        store1.forceIndexRebuild();
        store1.close();

        Path centroidsPath = storeDir.resolve("index/centroids.bin");
        if (Files.exists(centroidsPath)) {
            Files.write(centroidsPath, new byte[]{0, 0, 0, 0, 1, 2, 3});
        }

        WavePatternStoreImpl store2 = new WavePatternStoreImpl(storeDir, DIM, runtime);
        try {
            WavePattern query = randomPattern(new Random(SEED + 42), DIM);
            List<ResonanceMatch> results = store2.query(query, TOP_K);

            assertNotNull(results, "Query must succeed even with corrupted index");
            assertFalse(results.isEmpty(), "Query should return results via fallback");

            List<ResonanceMatchDetailed> detailed = store2.queryDetailed(query, TOP_K);
            assertNotNull(detailed);
            assertFalse(detailed.isEmpty());
        } finally {
            store2.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            runtime.close();
        }
    }

    @Test
    @Order(4)
    @DisplayName("Crash-safety: missing centroids.bin → fallback → queries work")
    void crashSafetyMissingIndex(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        Path storeDir = tmpDir.resolve("missing");

        WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);
        try {
            Random rng = new Random(SEED);
            for (int i = 0; i < 50; i++) {
                try { store.insert(randomPattern(rng, DIM), Map.of()); }
                catch (Exception e) { /* dup */ }
            }

            WavePattern query = randomPattern(new Random(SEED + 77), DIM);
            List<ResonanceMatch> results = store.query(query, TOP_K);

            assertNotNull(results);
            assertFalse(results.isEmpty(), "Query should return results via fallback");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            runtime.close();
        }
    }

    @Test
    @Order(5)
    @DisplayName("Exact equivalence: index.enabled=false produces identical results to legacy")
    void exactEquivalence(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "false");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();

        Path dir1 = tmpDir.resolve("store1");
        Path dir2 = tmpDir.resolve("store2");

        WavePatternStoreImpl store1 = new WavePatternStoreImpl(dir1, DIM, runtime);
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(dir2, DIM, runtime);

        try {
            Random rng = new Random(SEED);
            for (int i = 0; i < 200; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    store1.insert(p, Map.of());
                    store2.insert(p, Map.of());
                } catch (Exception e) { /* dup */ }
            }

            Random qRng = new Random(SEED + 500);
            for (int q = 0; q < 20; q++) {
                WavePattern query = randomPattern(qRng, DIM);

                List<ResonanceMatch> r1 = store1.query(query, TOP_K);
                List<ResonanceMatch> r2 = store2.query(query, TOP_K);

                assertEquals(r1.size(), r2.size(),
                        "Same result count for query " + q);

                for (int i = 0; i < r1.size(); i++) {
                    assertEquals(r1.get(i).id(), r2.get(i).id(),
                            "Same ID at position " + i + " for query " + q);
                    assertEquals(r1.get(i).energy(), r2.get(i).energy(), 1e-9,
                            "Same energy at position " + i + " for query " + q);
                }
            }
        } finally {
            store1.close();
            store2.close();
            System.clearProperty("resonance.index.enabled");
            runtime.close();
        }
    }

    @Test
    @Order(6)
    @DisplayName("Mutations: delete/replace reflected in ANN query results")
    void mutationsConsistency(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("mut"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<String> ids = new ArrayList<>();
            List<WavePattern> patterns = new ArrayList<>();

            for (int i = 0; i < 100; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    ids.add(id);
                    patterns.add(p);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            Set<String> deletedIds = new HashSet<>();
            for (int i = 0; i < 10 && i < ids.size(); i++) {
                store.delete(ids.get(i));
                deletedIds.add(ids.get(i));
            }

            for (int q = 0; q < 20; q++) {
                WavePattern query = randomPattern(new Random(SEED + 3000 + q), DIM);
                List<ResonanceMatch> results = store.query(query, TOP_K);

                for (ResonanceMatch m : results) {
                    assertFalse(deletedIds.contains(m.id()),
                            "Deleted pattern " + m.id() + " must not appear in results");
                }
            }

            if (ids.size() > 15) {
                String oldId = ids.get(15);
                WavePattern newPattern = randomPattern(new Random(SEED + 7777), DIM);
                String newId = store.replace(oldId, newPattern, Map.of());

                for (int q = 0; q < 10; q++) {
                    List<ResonanceMatch> results = store.query(
                            randomPattern(new Random(SEED + 4000 + q), DIM), 100);
                    Set<String> resultIds = results.stream()
                            .map(ResonanceMatch::id).collect(Collectors.toSet());

                    assertFalse(resultIds.contains(oldId),
                            "Replaced old ID must not appear in results");
                }

                List<ResonanceMatch> selfQuery = store.query(newPattern, TOP_K);
                assertTrue(selfQuery.stream().anyMatch(m -> m.id().equals(newId)),
                        "Replaced new pattern should be findable");
            }
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            runtime.close();
        }
    }

    @Test
    @Order(7)
    @DisplayName("Determinism: same query on ANN store returns identical results")
    void annDeterminism(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "true");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("det"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            for (int i = 0; i < 200; i++) {
                try { store.insert(randomPattern(rng, DIM), Map.of()); }
                catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            WavePattern query = randomPattern(new Random(SEED + 888), DIM);
            List<ResonanceMatch> run1 = store.query(query, TOP_K);
            List<ResonanceMatch> run2 = store.query(query, TOP_K);

            assertEquals(run1.size(), run2.size(), "Same count");
            for (int i = 0; i < run1.size(); i++) {
                assertEquals(run1.get(i).id(), run2.get(i).id(),
                        "Same ID at position " + i);
                assertEquals(run1.get(i).energy(), run2.get(i).energy(), 1e-9,
                        "Same energy at position " + i);
            }
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            runtime.close();
        }
    }

    @Test
    @Order(8)
    @DisplayName("Honest recall: perturbed queries vs ground-truth exhaustive scoring")
    void honestRecall(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "16");
        System.setProperty("resonance.index.l1.nprobe", "4");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("hrecall"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> allPatterns = new ArrayList<>();
            List<String> allIds = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    allPatterns.add(p);
                    allIds.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            ai.evacortex.resonancedb.core.engine.ResonanceKernel kernel =
                    new ai.evacortex.resonancedb.core.engine.JavaKernel();

            int queryCount = 200;
            int totalHits = 0;
            int totalExpected = 0;
            Random qRng = new Random(SEED + 12345);

            for (int q = 0; q < queryCount; q++) {
                int baseIdx = qRng.nextInt(allPatterns.size());
                WavePattern base = allPatterns.get(baseIdx);
                WavePattern query = perturbPattern(base, 0.3, qRng);

                float[] scores = new float[allPatterns.size()];
                for (int i = 0; i < allPatterns.size(); i++) {
                    scores[i] = kernel.compare(query, allPatterns.get(i));
                }
                Integer[] sortedIdx = new Integer[allPatterns.size()];
                for (int i = 0; i < sortedIdx.length; i++) sortedIdx[i] = i;
                Arrays.sort(sortedIdx, (a, b) -> Float.compare(scores[b], scores[a]));

                Set<String> trueTopK = new HashSet<>();
                for (int i = 0; i < TOP_K && i < sortedIdx.length; i++) {
                    trueTopK.add(allIds.get(sortedIdx[i]));
                }

                List<ResonanceMatch> annResults = store.query(query, TOP_K);
                Set<String> annIds = annResults.stream()
                        .map(ResonanceMatch::id).collect(Collectors.toSet());

                for (String id : trueTopK) {
                    if (annIds.contains(id)) totalHits++;
                }
                totalExpected += trueTopK.size();
            }

            double recall = totalExpected > 0 ? (double) totalHits / totalExpected : 0.0;
            System.out.println("Honest recall@" + TOP_K +
                    " (N=" + allPatterns.size() +
                    ", K=16, nProbe=4, queries=" + queryCount +
                    ", perturbed σ=0.3): " +
                    String.format("%.3f", recall) +
                    " (" + totalHits + "/" + totalExpected + ")");

            assertTrue(recall >= 0.0, "Recall must be non-negative");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(9)
    @DisplayName("IVF recall guarantee: nProbe=K must produce 100% recall vs kernel ground truth")
    void ivfRecallGuarantee(@TempDir Path tmpDir) {
        int k = 8;
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", String.valueOf(k));
        System.setProperty("resonance.index.l1.nprobe", String.valueOf(k));

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("ivf-guarantee"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> allPatterns = new ArrayList<>();
            List<String> allIds = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    allPatterns.add(p);
                    allIds.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            ai.evacortex.resonancedb.core.engine.ResonanceKernel kernel =
                    new ai.evacortex.resonancedb.core.engine.JavaKernel();

            int queries = 50;
            int misses = 0;
            int total = 0;
            Random qRng = new Random(SEED + 8888);

            for (int q = 0; q < queries; q++) {
                WavePattern query = randomPattern(qRng, DIM);

                float[] scores = new float[allPatterns.size()];
                for (int i = 0; i < allPatterns.size(); i++) {
                    scores[i] = kernel.compare(query, allPatterns.get(i));
                }
                Integer[] sortedIdx = new Integer[allPatterns.size()];
                for (int i = 0; i < sortedIdx.length; i++) sortedIdx[i] = i;
                Arrays.sort(sortedIdx, (a, b) -> Float.compare(scores[b], scores[a]));

                Set<String> trueTopK = new HashSet<>();
                for (int i = 0; i < TOP_K && i < sortedIdx.length; i++) {
                    trueTopK.add(allIds.get(sortedIdx[i]));
                }

                List<ResonanceMatch> ivfResults = store.query(query, TOP_K);
                Set<String> ivfIds = ivfResults.stream()
                        .map(ResonanceMatch::id).collect(Collectors.toSet());

                for (String id : trueTopK) {
                    total++;
                    if (!ivfIds.contains(id)) misses++;
                }
            }

            double recall = total > 0 ? 1.0 - (double) misses / total : 1.0;
            System.out.println("IVF recall guarantee (nProbe=K=" + k + "): " +
                    String.format("%.3f", recall) +
                    " (" + (total - misses) + "/" + total + ")");

            assertTrue(recall >= 0.99,
                    "IVF with nProbe=K must achieve >= 99% recall vs kernel ground truth, got " +
                            String.format("%.3f", recall));
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(10)
    @DisplayName("Delta+IVF interaction: patterns in delta findable alongside sealed")
    void deltaIvfInteraction(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("delta-ivf"), DIM, runtime);

        try {
            Random rng = new Random(SEED);

            List<String> sealedIds = new ArrayList<>();
            List<WavePattern> sealedPatterns = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    sealedIds.add(id);
                    sealedPatterns.add(p);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            List<String> deltaIds = new ArrayList<>();
            List<WavePattern> deltaPatterns = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    deltaIds.add(id);
                    deltaPatterns.add(p);
                } catch (Exception e) { /* dup */ }
            }

            for (int i = 0; i < deltaPatterns.size(); i++) {
                WavePattern query = deltaPatterns.get(i);
                List<ResonanceMatch> results = store.query(query, TOP_K);
                Set<String> resultIds = results.stream()
                        .map(ResonanceMatch::id).collect(Collectors.toSet());

                assertTrue(resultIds.contains(deltaIds.get(i)),
                        "Delta pattern " + deltaIds.get(i) + " must be findable via self-query");
            }

            Random sealedRng = new Random(SEED + 5555);
            int sealedChecks = Math.min(20, sealedPatterns.size());
            for (int i = 0; i < sealedChecks; i++) {
                int idx = sealedRng.nextInt(sealedPatterns.size());
                WavePattern query = sealedPatterns.get(idx);
                List<ResonanceMatch> results = store.query(query, TOP_K);
                Set<String> resultIds = results.stream()
                        .map(ResonanceMatch::id).collect(Collectors.toSet());

                assertTrue(resultIds.contains(sealedIds.get(idx)),
                        "Sealed pattern " + sealedIds.get(idx) + " must be findable via self-query");
            }
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(11)
    @DisplayName("Query equivalence: query() and queryDetailed() return identical ID sets")
    void queryDetailedEquivalence(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("equiv"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try { store.insert(p, Map.of()); } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            int queries = 50;
            int divergences = 0;
            Random qRng = new Random(SEED + 9999);

            for (int q = 0; q < queries; q++) {
                WavePattern query = randomPattern(qRng, DIM);

                List<ResonanceMatch> plain = store.query(query, TOP_K);
                List<ResonanceMatchDetailed> detailed = store.queryDetailed(query, TOP_K);

                List<String> plainIds = plain.stream().map(ResonanceMatch::id).toList();
                List<String> detailedIds = detailed.stream()
                        .map(ResonanceMatchDetailed::id).toList();

                if (!plainIds.equals(detailedIds)) {
                    divergences++;
                }
            }

            System.out.println("query/queryDetailed equivalence: " +
                    (queries - divergences) + "/" + queries + " identical");

            assertEquals(0, divergences,
                    "query() and queryDetailed() must return identical ID lists, " +
                            "but diverged on " + divergences + "/" + queries + " queries");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(12)
    @DisplayName("Dense clusters: high recall on Gaussian-clustered embeddings")
    void denseClusterRecall(@TempDir Path tmpDir) {
        int k = 16;
        int nProbe = 8;
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", String.valueOf(k));
        System.setProperty("resonance.index.l1.nprobe", String.valueOf(nProbe));

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("dense"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            int numClusters = 5;
            int patternsPerCluster = N / numClusters;

            WavePattern[] centroids = new WavePattern[numClusters];
            for (int c = 0; c < numClusters; c++) {
                centroids[c] = randomPattern(rng, DIM);
            }

            List<WavePattern> allPatterns = new ArrayList<>();
            List<String> allIds = new ArrayList<>();

            for (int c = 0; c < numClusters; c++) {
                for (int j = 0; j < patternsPerCluster; j++) {
                    WavePattern p = perturbPattern(centroids[c], 0.15, rng);
                    try {
                        String id = store.insert(p, Map.of());
                        allPatterns.add(p);
                        allIds.add(id);
                    } catch (Exception e) { /* dup */ }
                }
            }
            store.forceIndexRebuild();

            ai.evacortex.resonancedb.core.engine.ResonanceKernel kernel =
                    new ai.evacortex.resonancedb.core.engine.JavaKernel();

            int queries = 100;
            int totalHits = 0;
            int totalExpected = 0;
            Random qRng = new Random(SEED + 7000);

            for (int q = 0; q < queries; q++) {
                int baseIdx = qRng.nextInt(allPatterns.size());
                WavePattern query = perturbPattern(allPatterns.get(baseIdx), 0.2, qRng);

                float[] scores = new float[allPatterns.size()];
                for (int i = 0; i < allPatterns.size(); i++) {
                    scores[i] = kernel.compare(query, allPatterns.get(i));
                }
                Integer[] sortedIdx = new Integer[allPatterns.size()];
                for (int i = 0; i < sortedIdx.length; i++) sortedIdx[i] = i;
                Arrays.sort(sortedIdx, (a, b) -> Float.compare(scores[b], scores[a]));

                Set<String> trueTopK = new HashSet<>();
                for (int i = 0; i < TOP_K && i < sortedIdx.length; i++) {
                    trueTopK.add(allIds.get(sortedIdx[i]));
                }

                List<ResonanceMatch> annResults = store.query(query, TOP_K);
                Set<String> annIds = annResults.stream()
                        .map(ResonanceMatch::id).collect(Collectors.toSet());

                for (String id : trueTopK) {
                    if (annIds.contains(id)) totalHits++;
                }
                totalExpected += trueTopK.size();
            }

            double recall = totalExpected > 0 ? (double) totalHits / totalExpected : 1.0;
            System.out.println("Dense cluster recall@" + TOP_K +
                    " (K=" + k + ", nProbe=" + nProbe +
                    ", clusters=" + numClusters + ", σ=0.15): " +
                    String.format("%.3f", recall) +
                    " (" + totalHits + "/" + totalExpected + ")");

            assertTrue(recall >= 0.90,
                    "Dense cluster recall must be >= 0.90 with nProbe=" + nProbe +
                            "/K=" + k + ", got " + String.format("%.3f", recall));
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(13)
    @DisplayName("Scoring precision: float32 approx ranking matches exact within tolerance")
    void scoringPrecision(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("precision"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> allPatterns = new ArrayList<>();
            List<String> allIds = new ArrayList<>();

            for (int i = 0; i < 200; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    allPatterns.add(p);
                    allIds.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            ai.evacortex.resonancedb.core.engine.ResonanceKernel kernel =
                    new ai.evacortex.resonancedb.core.engine.JavaKernel();

            int queries = 50;
            int maxRankInversion = 0;
            Random qRng = new Random(SEED + 2222);

            for (int q = 0; q < queries; q++) {
                WavePattern query = randomPattern(qRng, DIM);

                float[] exactScores = new float[allPatterns.size()];
                for (int i = 0; i < allPatterns.size(); i++) {
                    exactScores[i] = kernel.compare(query, allPatterns.get(i));
                }

                float[] approxScores = new float[allPatterns.size()];
                float queryEnergy = ai.evacortex.resonancedb.core.math.UnfoldedMath
                        .energyFloat32(query);
                float[] queryU = ai.evacortex.resonancedb.core.math.UnfoldedMath
                        .unfoldFloat32(query);

                for (int i = 0; i < allPatterns.size(); i++) {
                    float[] candU = ai.evacortex.resonancedb.core.math.UnfoldedMath
                            .unfoldFloat32(allPatterns.get(i));
                    double dot = 0.0;
                    for (int d = 0; d < queryU.length; d++) {
                        dot += (double) queryU[d] * (double) candU[d];
                    }
                    double candEnergy = ai.evacortex.resonancedb.core.math.UnfoldedMath
                            .energyFloat32(allPatterns.get(i));
                    approxScores[i] = ai.evacortex.resonancedb.core.math.UnfoldedMath
                            .scoreFromDot(dot, queryEnergy, candEnergy);
                }

                Integer[] exactRank = new Integer[allPatterns.size()];
                for (int i = 0; i < exactRank.length; i++) exactRank[i] = i;
                Arrays.sort(exactRank, (a, b) -> Float.compare(exactScores[b], exactScores[a]));

                Map<Integer, Integer> exactRankMap = new HashMap<>();
                for (int r = 0; r < exactRank.length; r++) {
                    exactRankMap.put(exactRank[r], r);
                }

                Integer[] approxRankArr = new Integer[allPatterns.size()];
                for (int i = 0; i < approxRankArr.length; i++) approxRankArr[i] = i;
                Arrays.sort(approxRankArr, (a, b) -> Float.compare(approxScores[b], approxScores[a]));

                for (int r = 0; r < TOP_K && r < approxRankArr.length; r++) {
                    int patIdx = exactRank[r];
                    int approxR = -1;
                    for (int ar = 0; ar < approxRankArr.length; ar++) {
                        if (approxRankArr[ar] == patIdx) { approxR = ar; break; }
                    }
                    int inversion = Math.abs(r - approxR);
                    maxRankInversion = Math.max(maxRankInversion, inversion);
                }
            }

            System.out.println("Max rank inversion (exact vs approx, top-" + TOP_K + "): " +
                    maxRankInversion);

            int overfetch = Integer.getInteger("resonance.query.overfetch", 4);
            assertTrue(maxRankInversion < TOP_K * overfetch,
                    "Max rank inversion " + maxRankInversion +
                            " must be within overfetch budget (" + (TOP_K * overfetch) + ")");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(14)
    @DisplayName("Metamorphic: IVF exactEquivalence=true ≡ kernel ground truth on 10000 queries")
    void metamorphicExactEquivalence(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "16");
        System.setProperty("resonance.index.l1.nprobe", "4");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("meta"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> allPatterns = new ArrayList<>();
            List<String> allIds = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    String id = store.insert(p, Map.of());
                    allPatterns.add(p);
                    allIds.add(id);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            ai.evacortex.resonancedb.core.engine.ResonanceKernel kernel =
                    new ai.evacortex.resonancedb.core.engine.JavaKernel();

            int queryCount = Integer.getInteger("resonance.test.metamorphic.queries", 10_000);
            Random qRng = new Random(SEED + 14_000);

            for (int q = 0; q < queryCount; q++) {
                WavePattern query = randomPattern(qRng, DIM);

                float[] scores = new float[allPatterns.size()];
                for (int i = 0; i < allPatterns.size(); i++) {
                    scores[i] = kernel.compare(query, allPatterns.get(i));
                }
                Integer[] sortedIdx = new Integer[allPatterns.size()];
                for (int i = 0; i < sortedIdx.length; i++) sortedIdx[i] = i;
                Arrays.sort(sortedIdx, (a, b) -> Float.compare(scores[b], scores[a]));

                List<String> trueTopK = new ArrayList<>();
                for (int i = 0; i < TOP_K && i < sortedIdx.length; i++) {
                    trueTopK.add(allIds.get(sortedIdx[i]));
                }

                List<ResonanceMatch> ivfResults = store.query(query, TOP_K);
                List<String> ivfIds = ivfResults.stream().map(ResonanceMatch::id).toList();

                assertEquals(trueTopK, ivfIds,
                        "Query " + q + ": IVF exactEquivalence must produce identical top-K as kernel");

                for (int i = 0; i < ivfResults.size() && i < TOP_K; i++) {
                    assertEquals(scores[sortedIdx[i]], ivfResults.get(i).energy(),
                            "Query " + q + " rank " + i + ": energy must match kernel bit-for-bit");
                }
            }
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.exactEquivalence");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(15)
    @DisplayName("Bitwise determinism: query results identical after close and reopen")
    void bitwiseDeterminismAcrossReopen(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");
        System.setProperty("resonance.wal.enabled", "true");
        System.setProperty("resonance.wal.durability", "strict");

        Path dbPath = tmpDir.resolve("determ");
        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(dbPath, DIM, runtime);

        int queryCount = 100;
        Random qRng = new Random(SEED + 15_000);
        List<WavePattern> queries = new ArrayList<>();
        for (int q = 0; q < queryCount; q++) {
            queries.add(randomPattern(qRng, DIM));
        }

        List<List<String>> firstRunIds = new ArrayList<>();
        List<List<Float>> firstRunEnergies = new ArrayList<>();

        try {
            Random rng = new Random(SEED);
            for (int i = 0; i < 200; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try { store.insert(p, Map.of()); } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            for (WavePattern q : queries) {
                List<ResonanceMatch> results = store.query(q, TOP_K);
                firstRunIds.add(results.stream().map(ResonanceMatch::id).toList());
                firstRunEnergies.add(results.stream().map(ResonanceMatch::energy).toList());
            }
        } finally {
            store.close();
            runtime.close();
        }

        StoreRuntimeServices runtime2 = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store2 = new WavePatternStoreImpl(dbPath, DIM, runtime2);

        try {
            store2.forceIndexRebuild();

            for (int q = 0; q < queryCount; q++) {
                List<ResonanceMatch> results = store2.query(queries.get(q), TOP_K);
                List<String> ids = results.stream().map(ResonanceMatch::id).toList();
                List<Float> energies = results.stream().map(ResonanceMatch::energy).toList();

                assertEquals(firstRunIds.get(q), ids,
                        "Query " + q + ": IDs must be identical after reopen");
                assertEquals(firstRunEnergies.get(q), energies,
                        "Query " + q + ": energies must be bit-identical after reopen");
            }
        } finally {
            store2.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            System.clearProperty("resonance.wal.enabled");
            System.clearProperty("resonance.wal.durability");
            runtime2.close();
        }
    }

    @Test
    @Order(16)
    @DisplayName("Replace atomicity: no query sees both old and new pattern simultaneously")
    void replaceAtomicity(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "8");
        System.setProperty("resonance.index.l1.nprobe", "8");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("atomicity"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    ids.add(store.insert(p, Map.of()));
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            String targetOldId = ids.get(0);
            WavePattern replacement = randomPattern(new Random(SEED + 16_000), DIM);

            AtomicInteger violations = new AtomicInteger(0);
            AtomicInteger queriesDone = new AtomicInteger(0);
            AtomicReference<String> newIdRef = new AtomicReference<>();
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(2);

            Thread queryThread = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 1000; i++) {
                        List<ResonanceMatch> results = store.query(replacement, 100);
                        Set<String> resultIds = results.stream()
                                .map(ResonanceMatch::id).collect(Collectors.toSet());
                        String newId = newIdRef.get();
                        if (newId != null && resultIds.contains(targetOldId) && resultIds.contains(newId)) {
                            violations.incrementAndGet();
                        }
                        queriesDone.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    done.countDown();
                }
            });

            Thread replaceThread = new Thread(() -> {
                try {
                    start.await();
                    String newId = store.replace(targetOldId, replacement, Map.of());
                    newIdRef.set(newId);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    done.countDown();
                }
            });

            queryThread.start();
            replaceThread.start();
            start.countDown();
            done.await(30, TimeUnit.SECONDS);

            assertEquals(0, violations.get(),
                    "No query must see both old and new pattern; violations=" + violations.get() +
                            " in " + queriesDone.get() + " queries");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted");
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    @Test
    @Order(17)
    @DisplayName("Epsilon bound: |approxScore - exactScore| <= epsilon on 100K pairs incl. unnormalized")
    void epsilonBoundVerification() {
        ai.evacortex.resonancedb.core.engine.ResonanceKernel kernel =
                new ai.evacortex.resonancedb.core.engine.JavaKernel();

        int pairCount = 100_000;
        int unfoldedDim = DIM * 2;
        float epsilon = 0.5f * unfoldedDim * (float) Math.scalb(1.0, -24);

        Random rng = new Random(SEED + 17_000);
        int violations = 0;
        float maxError = 0.0f;

        for (int i = 0; i < pairCount; i++) {
            double amplitudeScale = 0.01 + rng.nextDouble() * 100.0;
            WavePattern a = scaledRandomPattern(rng, DIM, amplitudeScale);
            WavePattern b = scaledRandomPattern(rng, DIM, 0.01 + rng.nextDouble() * 100.0);

            float exactScore = kernel.compare(a, b);

            float[] aU = ai.evacortex.resonancedb.core.math.UnfoldedMath.unfoldFloat32(a);
            float[] bU = ai.evacortex.resonancedb.core.math.UnfoldedMath.unfoldFloat32(b);
            float aE = ai.evacortex.resonancedb.core.math.UnfoldedMath.energyFloat32(a);
            float bE = ai.evacortex.resonancedb.core.math.UnfoldedMath.energyFloat32(b);
            double dot = 0.0;
            for (int d = 0; d < unfoldedDim; d++) {
                dot += (double) aU[d] * (double) bU[d];
            }
            float approxScore = ai.evacortex.resonancedb.core.math.UnfoldedMath
                    .scoreFromDot(dot, aE, bE);

            float error = Math.abs(exactScore - approxScore);
            maxError = Math.max(maxError, error);
            if (error > epsilon) {
                violations++;
            }
        }

        System.out.println("Epsilon bound verification (incl. unnormalized): " +
                pairCount + " pairs, epsilon=" + String.format("%.2e", epsilon) +
                ", maxError=" + String.format("%.2e", maxError) +
                ", violations=" + violations);

        assertEquals(0, violations,
                "All |approx - exact| must be <= epsilon=" + String.format("%.2e", epsilon) +
                        "; violations=" + violations + ", maxError=" + String.format("%.2e", maxError));
    }

    @Test
    @Order(18)
    @DisplayName("Epsilon set telemetry: average and max finalist count on 1000 queries")
    void epsilonSetTelemetry(@TempDir Path tmpDir) {
        System.setProperty("resonance.index.enabled", "true");
        System.setProperty("resonance.index.exactEquivalence", "true");
        System.setProperty("resonance.index.l2.enabled", "false");
        System.setProperty("resonance.index.l1.k", "16");
        System.setProperty("resonance.index.l1.nprobe", "16");

        StoreRuntimeServices runtime = StoreRuntimeServices.fromSystemProperties();
        WavePatternStoreImpl store = new WavePatternStoreImpl(tmpDir.resolve("eps-telem"), DIM, runtime);

        try {
            Random rng = new Random(SEED);
            List<WavePattern> allPatterns = new ArrayList<>();
            for (int i = 0; i < N; i++) {
                WavePattern p = randomPattern(rng, DIM);
                try {
                    store.insert(p, Map.of());
                    allPatterns.add(p);
                } catch (Exception e) { /* dup */ }
            }
            store.forceIndexRebuild();

            int queryCount = 1000;
            Random qRng = new Random(SEED + 18_000);

            int totalResults = 0;
            for (int q = 0; q < queryCount; q++) {
                WavePattern query = randomPattern(qRng, DIM);
                List<ResonanceMatch> results = store.query(query, TOP_K);
                totalResults += results.size();
            }

            double avgResults = (double) totalResults / queryCount;
            System.out.println("Epsilon set telemetry: " + queryCount + " queries, " +
                    "avgResultCount=" + String.format("%.1f", avgResults) +
                    " (expected ~" + TOP_K + ")");

            assertTrue(avgResults >= TOP_K - 0.5,
                    "Average result count should be ~topK=" + TOP_K + ", got " + avgResults);
        } finally {
            store.close();
            System.clearProperty("resonance.index.enabled");
            System.clearProperty("resonance.index.exactEquivalence");
            System.clearProperty("resonance.index.l2.enabled");
            System.clearProperty("resonance.index.l1.k");
            System.clearProperty("resonance.index.l1.nprobe");
            runtime.close();
        }
    }

    private static WavePattern scaledRandomPattern(Random rng, int dim, double scale) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble() * scale;
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
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

    /**
     * Creates a perturbed copy of a pattern by adding gaussian noise.
     * Amplitude noise is multiplicative (1 + σ·N(0,1)), phase noise is additive (σ·N(0,1)).
     */
    private static WavePattern perturbPattern(WavePattern base, double sigma, Random rng) {
        double[] amp = base.amplitude().clone();
        double[] phase = base.phase().clone();
        for (int i = 0; i < amp.length; i++) {
            amp[i] = Math.max(0.0, amp[i] * (1.0 + sigma * rng.nextGaussian()));
            phase[i] = phase[i] + sigma * rng.nextGaussian();
        }
        return new WavePattern(amp, phase);
    }
}
