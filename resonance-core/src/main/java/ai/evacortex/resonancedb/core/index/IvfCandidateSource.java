/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.math.UnfoldedMath;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.io.CachedReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public final class IvfCandidateSource implements CandidateSource {

    private final AtomicReference<CentroidIndex> indexRef;
    private final DeltaIndex delta;
    private final ManifestIndex manifest;
    private final int nProbe;
    private final int efSearch;
    private final CandidateSource fallback;

    private final AtomicReference<Map<Integer, VamanaGraph>> vamanaGraphs =
            new AtomicReference<>(Map.of());

    private volatile Function<String, CachedReader> readerProvider;

    private volatile PostingSidecar.Loaded postingSidecar;
    private volatile Path sidecarPathRef;

    private volatile boolean shutdown;

    public IvfCandidateSource(CentroidIndex centroidIndex,
                              DeltaIndex delta,
                              ManifestIndex manifest,
                              int nProbe,
                              CandidateSource fallback) {
        this.indexRef = new AtomicReference<>(centroidIndex);
        this.delta = Objects.requireNonNull(delta);
        this.manifest = Objects.requireNonNull(manifest);
        this.nProbe = nProbe;
        this.efSearch = Integer.getInteger("resonance.index.l2.efsearch", 96);
        this.fallback = Objects.requireNonNull(fallback);
    }

    public void requestShutdown() {
        this.shutdown = true;
    }

    public boolean isShutdownRequested() {
        return shutdown;
    }

    public void setSidecarPath(Path path) {
        this.sidecarPathRef = path;
    }

    public boolean loadSidecar() {
        Path path = sidecarPathRef;
        if (path == null) return false;
        try {
            PostingSidecar.Loaded loaded = PostingSidecar.load(path);
            if (loaded != null) {
                replaceSidecar(loaded);
                return true;
            }
        } catch (Exception e) {
            System.err.println("Failed to load sidecar: " + e.getMessage());
        }
        return false;
    }

    @Override
    public Map<String, Collection<String>> candidates(WavePattern query, int topK) {
        CentroidIndex index = indexRef.get();

        if (index == null) {
            return fallback.candidates(query, topK);
        }

        Set<String> annCandidates;

        Map<Integer, VamanaGraph> graphs = vamanaGraphs.get();

        annCandidates = index.queryCandidates(query, nProbe);

        if (!graphs.isEmpty() && readerProvider != null) {
            double[] queryU = UnfoldedMath.unfold(query);
            UnfoldedMath.l2Normalize(queryU);
            int[] topPartitions = SphericalKMeans.topNearestCentroids(
                    queryU, index.centroids(), index.dim(), nProbe);

            for (int partIdx : topPartitions) {
                VamanaGraph graph = graphs.get(partIdx);
                if (graph == null || graph.nodeCount() == 0) continue;

                java.util.function.IntFunction<WavePattern> loader = nodeIdx -> {
                    if (nodeIdx < 0 || nodeIdx >= graph.nodeCount()) return null;
                    String patId = graph.patternIds()[nodeIdx];
                    ManifestIndex.PatternLocation loc = manifest.get(patId);
                    if (loc == null) return null;
                    try {
                        CachedReader reader = readerProvider.apply(loc.segmentName());
                        return (reader != null) ? reader.readById(patId) : null;
                    } catch (RuntimeException e) {
                        return null;
                    }
                };

                List<String> partitionCandidates = graph.search(
                        query, loader, efSearch, graph.nodeCount());
                annCandidates.addAll(partitionCandidates);
            }
        }

        annCandidates.addAll(delta.allIds());

        annCandidates.removeIf(delta::isDeleted);

        return groupBySegment(annCandidates);
    }

    @Override
    public ScoredCandidates scoredCandidates(WavePattern query, int topK) {
        CentroidIndex index = indexRef.get();
        PostingSidecar.Loaded sidecar = this.postingSidecar;

        if (index == null || sidecar == null) {
            return CandidateSource.super.scoredCandidates(query, topK);
        }

        float[] queryU = UnfoldedMath.unfoldFloat32(query);
        float queryEnergy = UnfoldedMath.energyFloat32(query);

        double[] queryUDouble = UnfoldedMath.unfold(query);
        UnfoldedMath.l2Normalize(queryUDouble);
        int[] topPartitions = SphericalKMeans.topNearestCentroids(
                queryUDouble, index.centroids(), index.dim(), nProbe);

        List<PostingSidecar.ScoredCandidate> results = new ArrayList<>();

        for (int partIdx : topPartitions) {
            sidecar.scanPartition(partIdx, queryU, queryEnergy, results);
        }

        results.removeIf(sc -> delta.isDeleted(sc.id()));

        Set<String> scoredIds = new HashSet<>(results.size());
        for (PostingSidecar.ScoredCandidate sc : results) {
            scoredIds.add(sc.id());
        }

        Map<Integer, VamanaGraph> graphs = vamanaGraphs.get();
        if (!graphs.isEmpty() && readerProvider != null) {
            for (int partIdx : topPartitions) {
                VamanaGraph graph = graphs.get(partIdx);
                if (graph == null || graph.nodeCount() == 0) continue;

                java.util.function.IntFunction<WavePattern> loader = nodeIdx -> {
                    if (nodeIdx < 0 || nodeIdx >= graph.nodeCount()) return null;
                    String patId = graph.patternIds()[nodeIdx];
                    ManifestIndex.PatternLocation loc = manifest.get(patId);
                    if (loc == null) return null;
                    try {
                        CachedReader reader = readerProvider.apply(loc.segmentName());
                        return (reader != null) ? reader.readById(patId) : null;
                    } catch (RuntimeException e) {
                        return null;
                    }
                };

                for (String id : graph.search(query, loader, efSearch, graph.nodeCount())) {
                    if (!scoredIds.contains(id) && !delta.isDeleted(id)) {
                        results.add(PostingSidecar.ScoredCandidate.forExactRescore(id));
                        scoredIds.add(id);
                    }
                }
            }
        }

        Set<String> deltaIds = delta.allIds();
        for (String id : deltaIds) {
            if (!scoredIds.contains(id)) {
                results.add(PostingSidecar.ScoredCandidate.forExactRescore(id));
            }
        }

        return new ScoredCandidates(results, false);
    }

    @Override
    public boolean isExhaustive() {
        return indexRef.get() == null;
    }

    public void buildIndex(Function<String, CachedReader> readerProvider,
                           Iterable<String> segmentNames,
                           int patternLen,
                           long seed) {
        if (shutdown) return;
        int unfoldedDim = 2 * patternLen;
        int maxSample = safeParseInt("resonance.index.l1.sampleSize", 20_000);
        double lambda = safeParseDouble("resonance.index.l1.lambda", 0.15);

        List<double[]> sampleVectors = new ArrayList<>();
        int totalCount = 0;
        Random sampleRng = new Random(seed);

        List<String> allValidIds = new ArrayList<>();
        Map<String, String> idToSegment = new HashMap<>();
        for (String segName : segmentNames) {
            CachedReader reader = safeGetReader(readerProvider, segName);
            if (reader == null) continue;
            for (String id : safeGetIds(reader)) {
                if (manifest.contains(id)) {
                    allValidIds.add(id);
                    idToSegment.put(id, segName);
                }
            }
        }
        totalCount = allValidIds.size();
        if (totalCount == 0) return;

        List<String> sampleIdList;
        if (totalCount <= maxSample) {
            sampleIdList = allValidIds;
        } else {
            List<String> shuffled = new ArrayList<>(allValidIds);
            for (int i = 0; i < maxSample; i++) {
                int j = i + sampleRng.nextInt(totalCount - i);
                String tmp = shuffled.get(i);
                shuffled.set(i, shuffled.get(j));
                shuffled.set(j, tmp);
            }
            sampleIdList = shuffled.subList(0, maxSample);
        }

        for (String id : sampleIdList) {
            WavePattern p = safeReadPattern(readerProvider, idToSegment.get(id), id, patternLen);
            if (p == null) continue;
            double[] u = unfoldAndNormalize(p);
            if (u != null) {
                sampleVectors.add(u);
            }
        }
        if (sampleVectors.isEmpty()) return;

        int kDefault = SphericalKMeans.recommendedK(totalCount);
        int k = safeParseInt("resonance.index.l1.k", kDefault);
        if (k <= 0) k = kDefault;
        if (k > sampleVectors.size()) k = sampleVectors.size();

        double[][] sampleData = sampleVectors.toArray(new double[0][]);
        sampleVectors = null;
        double[][] centroids = SphericalKMeans.fit(sampleData, k, unfoldedDim, seed);
        sampleData = null;

        CentroidIndex newIndex = new CentroidIndex(centroids, unfoldedDim, lambda);
        Map<Integer, PostingSidecar.PartitionData> partitions = new HashMap<>();
        for (int i = 0; i < k; i++) {
            partitions.put(i, new PostingSidecar.PartitionData());
        }

        for (String id : allValidIds) {
            if (shutdown) return;
            WavePattern p = safeReadPattern(readerProvider, idToSegment.get(id), id, patternLen);
            if (p == null) continue;
            double[] u = unfoldAndNormalize(p);
            if (u == null) continue;

            newIndex.assignNormalized(id, u);

            int nearest = SphericalKMeans.nearestCentroid(u, centroids, unfoldedDim);
            partitions.get(nearest).add(id, UnfoldedMath.unfoldFloat32(p), UnfoldedMath.energyFloat32(p));
        }
        allValidIds = null;
        idToSegment = null;

        Path sidecarPath = this.sidecarPathRef;
        if (sidecarPath != null) {
            List<PostingSidecar.PartitionData> partitionList = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                partitionList.add(partitions.getOrDefault(i, new PostingSidecar.PartitionData()));
            }
            partitions = null;

            try {
                PostingSidecar.write(sidecarPath, partitionList, unfoldedDim);
                replaceSidecar(PostingSidecar.load(sidecarPath));
            } catch (Exception e) {
                System.err.println("Failed to write/load sidecar: " + e.getMessage());
                replaceSidecar(null);
            }
        }

        indexRef.set(newIndex);
        delta.drainAll();
    }

    private void replaceSidecar(PostingSidecar.Loaded newSidecar) {
        PostingSidecar.Loaded prev = this.postingSidecar;
        this.postingSidecar = newSidecar;
        if (prev != null) prev.close();
    }

    private CachedReader safeGetReader(Function<String, CachedReader> provider, String segName) {
        try { return provider.apply(segName); }
        catch (RuntimeException e) { return null; }
    }

    private Set<String> safeGetIds(CachedReader reader) {
        try { return reader.allIds(); }
        catch (RuntimeException e) { return Set.of(); }
    }

    private static double[] unfoldAndNormalize(WavePattern p) {
        double[] u = UnfoldedMath.unfold(p);
        double normSq = UnfoldedMath.dotUnfolded(u, u);
        if (!Double.isFinite(normSq) || normSq < UnfoldedMath.NORM_SQ_FLOOR) return null;
        UnfoldedMath.l2Normalize(u);
        return u;
    }

    private WavePattern safeReadPattern(Function<String, CachedReader> provider,
                                        String segName, String id, int patternLen) {
        CachedReader reader = safeGetReader(provider, segName);
        if (reader == null) return null;
        try {
            WavePattern p = reader.readById(id);
            return (p != null && p.amplitude().length == patternLen) ? p : null;
        } catch (RuntimeException e) { return null; }
    }

    public void buildIndexWithVamana(Function<String, CachedReader> readerProvider,
                                      Iterable<String> segmentNames,
                                      int patternLen,
                                      long seed) {
        buildIndex(readerProvider, segmentNames, patternLen, seed);

        CentroidIndex index = indexRef.get();
        if (index == null) return;

        boolean vamanaEnabled = Boolean.parseBoolean(
                System.getProperty("resonance.index.l2.enabled", "true"));
        if (!vamanaEnabled) return;

        int R = safeParseInt("resonance.index.l2.R", VamanaBuilder.DEFAULT_R);
        int Lbuild = safeParseInt("resonance.index.l2.Lbuild", VamanaBuilder.DEFAULT_L_BUILD);
        double alpha = safeParseDouble("resonance.index.l2.alpha", VamanaBuilder.DEFAULT_ALPHA);

        Map<Integer, List<String>> partitionIds = new HashMap<>();

        for (String segName : segmentNames) {
            CachedReader reader = safeGetReader(readerProvider, segName);
            if (reader == null) continue;

            for (String id : safeGetIds(reader)) {
                if (!manifest.contains(id)) continue;
                WavePattern p = safeReadPattern(readerProvider, segName, id, patternLen);
                if (p == null) continue;
                double[] u = unfoldAndNormalize(p);
                if (u == null) continue;
                int nearest = SphericalKMeans.nearestCentroid(u, index.centroids(), index.dim());
                partitionIds.computeIfAbsent(nearest, x -> new ArrayList<>()).add(id);
            }
        }

        Map<Integer, VamanaGraph> newGraphs = new ConcurrentHashMap<>();
        int maxPartitionSize = safeParseInt("resonance.index.l2.maxPartitionSize", 100_000);
        int minPartitionSizeForVamana = safeParseInt("resonance.index.l2.minPartitionSize", 2 * R);

        List<Map.Entry<Integer, List<String>>> eligible = new ArrayList<>();
        for (var entry : partitionIds.entrySet()) {
            List<String> ids = entry.getValue();
            if (ids.size() < minPartitionSizeForVamana) continue;
            if (ids.size() > maxPartitionSize) {
                System.err.println("Partition " + entry.getKey() + " exceeds Vamana size limit (" +
                        ids.size() + " > " + maxPartitionSize + "), using L1 only");
                continue;
            }
            eligible.add(entry);
        }

        if (shutdown) return;
        if (eligible.isEmpty()) {
            this.readerProvider = readerProvider;
            vamanaGraphs.set(Map.copyOf(newGraphs));
            return;
        }

        int parallelism = Math.min(eligible.size(),
                Math.max(1, Runtime.getRuntime().availableProcessors() - 1));
        var tasks = new java.util.concurrent.ForkJoinPool(parallelism);
        try {
            tasks.submit(() -> eligible.parallelStream().forEach(entry -> {
                if (shutdown) return;

                int partIdx = entry.getKey();
                List<String> ids = entry.getValue();

                String[] idArr = ids.toArray(new String[0]);
                WavePattern[] patArr = new WavePattern[idArr.length];
                int valid = 0;
                for (int i = 0; i < idArr.length; i++) {
                    if (shutdown) return;
                    ManifestIndex.PatternLocation loc = manifest.get(idArr[i]);
                    if (loc == null) continue;
                    try {
                        CachedReader reader = readerProvider.apply(loc.segmentName());
                        if (reader != null) {
                            patArr[i] = reader.readById(idArr[i]);
                            if (patArr[i] != null) valid++;
                        }
                    } catch (RuntimeException e) {
                        System.err.println("Failed to read pattern " + idArr[i] + ": " + e.getMessage());
                    }
                }
                if (valid < 2) return;

                if (valid < idArr.length) {
                    String[] compactIds = new String[valid];
                    WavePattern[] compactPats = new WavePattern[valid];
                    int j = 0;
                    for (int i = 0; i < idArr.length; i++) {
                        if (patArr[i] != null) {
                            compactIds[j] = idArr[i];
                            compactPats[j] = patArr[i];
                            j++;
                        }
                    }
                    idArr = compactIds;
                    patArr = compactPats;
                }

                VamanaGraph graph = VamanaBuilder.build(
                        idArr, patArr, R, Lbuild, alpha, seed + partIdx,
                        () -> shutdown);
                if (graph.medoid() >= 0) {
                    newGraphs.put(partIdx, graph);
                }
            })).get(10, TimeUnit.MINUTES);
        } catch (java.util.concurrent.TimeoutException e) {
            System.err.println("Parallel Vamana build timed out after 10 minutes");
        } catch (Exception e) {
            if (!shutdown) {
                System.err.println("Parallel Vamana build failed: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        } finally {
            tasks.shutdown();
            try {
                if (!tasks.awaitTermination(30, TimeUnit.SECONDS)) {
                    tasks.shutdownNow();
                    if (!tasks.awaitTermination(5, TimeUnit.SECONDS)) {
                        System.err.println("Vamana ForkJoinPool did not terminate");
                    }
                }
            } catch (InterruptedException e) {
                tasks.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (shutdown) return;

        this.readerProvider = readerProvider;

        vamanaGraphs.set(Map.copyOf(newGraphs));
    }

    public void persistIndex(Path centroidsPath) throws IOException {
        CentroidIndex index = indexRef.get();
        if (index != null) {
            CentroidPersistence.write(centroidsPath,
                    index.centroids(), index.dim(), index.lambda());
        }
    }

    public boolean loadIndex(Path centroidsPath) {
        CentroidIndex loaded = CentroidPersistence.read(centroidsPath);
        if (loaded != null) {
            indexRef.set(loaded);
            return true;
        }
        return false;
    }

    public void swapIndex(CentroidIndex newIndex) {
        indexRef.set(newIndex);
    }

    public CentroidIndex currentIndex() {
        return indexRef.get();
    }

    public DeltaIndex delta() {
        return delta;
    }

    public void rebuildSidecar(Function<String, CachedReader> readerProvider,
                               Iterable<String> segmentNames,
                               int patternLen) {
        CentroidIndex index = indexRef.get();
        if (index == null) return;

        Path sidecarPath = this.sidecarPathRef;
        if (sidecarPath == null) return;

        int unfoldedDim = 2 * patternLen;
        int k = index.centroids().length;

        Map<Integer, PostingSidecar.PartitionData> partitions = new HashMap<>();
        for (int i = 0; i < k; i++) {
            partitions.put(i, new PostingSidecar.PartitionData());
        }

        for (String segName : segmentNames) {
            CachedReader reader = safeGetReader(readerProvider, segName);
            if (reader == null) continue;

            for (String id : safeGetIds(reader)) {
                if (!manifest.contains(id)) continue;
                WavePattern p = safeReadPattern(readerProvider, segName, id, patternLen);
                if (p == null) continue;
                double[] u = unfoldAndNormalize(p);
                if (u == null) continue;
                int nearest = SphericalKMeans.nearestCentroid(u, index.centroids(), unfoldedDim);
                PostingSidecar.PartitionData pd = partitions.get(nearest);
                if (pd != null) pd.add(id, UnfoldedMath.unfoldFloat32(p), UnfoldedMath.energyFloat32(p));
            }
        }

        List<PostingSidecar.PartitionData> partitionList = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            partitionList.add(partitions.getOrDefault(i, new PostingSidecar.PartitionData()));
        }

        try {
            PostingSidecar.write(sidecarPath, partitionList, unfoldedDim);
            replaceSidecar(PostingSidecar.load(sidecarPath));
        } catch (Exception e) {
            System.err.println("rebuildSidecar failed: " + e.getMessage());
            replaceSidecar(null);
        }
    }

    private Map<String, Collection<String>> groupBySegment(Set<String> patternIds) {
        Map<String, Collection<String>> result = new LinkedHashMap<>();
        for (String id : patternIds) {
            ManifestIndex.PatternLocation loc = manifest.get(id);
            if (loc != null) {
                result.computeIfAbsent(loc.segmentName(), k -> new ArrayList<>()).add(id);
            }
        }
        return result;
    }

    private static int safeParseInt(String property, int defaultValue) {
        String value = System.getProperty(property);
        if (value == null || value.equalsIgnoreCase("auto")) return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static double safeParseDouble(String property, double defaultValue) {
        String value = System.getProperty(property);
        if (value == null) return defaultValue;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
