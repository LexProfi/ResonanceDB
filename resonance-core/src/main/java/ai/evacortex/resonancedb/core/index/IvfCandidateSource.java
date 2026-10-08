/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.engine.PhaseRoutingProfile;
import ai.evacortex.resonancedb.core.engine.PhaseWeights;
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
    private volatile Path momentsPathRef;
    private volatile ResonanceMoments moments;

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

    public void setMomentsPath(Path path) {
        this.momentsPathRef = path;
    }

    public boolean loadMoments() {
        Path path = momentsPathRef;
        if (path == null) return false;
        try {
            ResonanceMoments loaded = ResonanceMoments.load(path);
            if (loaded != null) {
                CentroidIndex index = indexRef.get();
                if (index != null && loaded.isCompatible(index.size(), index.dim() / 2)) {
                    this.moments = loaded;
                    return true;
                }
                System.err.println("Moments incompatible with current index, ignoring");
            }
        } catch (Exception e) {
            System.err.println("Failed to load moments: " + e.getMessage());
        }
        return false;
    }

    public ResonanceMoments currentMoments() {
        return moments;
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
        return scoredCandidates(query, topK, nProbe);
    }

    public ScoredCandidates scoredCandidates(WavePattern query, int topK, int effectiveNProbe) {
        CentroidIndex index = indexRef.get();
        PostingSidecar.Loaded sidecar = this.postingSidecar;

        if (index == null || sidecar == null) {
            return CandidateSource.super.scoredCandidates(query, topK);
        }

        int probeCount = Math.min(effectiveNProbe, index.size());

        float[] queryU = UnfoldedMath.unfoldFloat32(query);
        float queryEnergy = UnfoldedMath.energyFloat32(query);

        double[] queryUDouble = UnfoldedMath.unfold(query);
        UnfoldedMath.l2Normalize(queryUDouble);
        int[] topPartitions = SphericalKMeans.topNearestCentroids(
                queryUDouble, index.centroids(), index.dim(), probeCount);

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

    /** Maximum probe fraction for weighted queries (configurable). Default: 40% of centroids. */
    private static final double MAX_PROBE_FRACTION =
            safeParseDouble("resonance.index.weighted.maxProbePct", 0.40);

    /** Progressive probing round fractions of K. */
    private static final double[] PROBE_ROUNDS = {0.15, 0.25, 0.40};

    /**
     * Minimum moment score spread (top vs median centroid) to use bounded probing.
     * Below this threshold, moment routing has no discriminating power —
     * the cost model falls back to exhaustive scan.
     */
    private static final double MOMENT_SPREAD_THRESHOLD =
            safeParseDouble("resonance.index.weighted.momentSpreadMin", 0.01);

    /** Last query's probe statistics (diagnostic/benchmark only, not thread-safe). */
    private volatile WeightedProbeStats lastProbeStats;

    /** Diagnostic statistics for the last weighted query. */
    public record WeightedProbeStats(int totalCentroids, int visitedCentroids,
                                      int candidatesCollected, int rounds,
                                      boolean budgetExhausted) {
        public double probeFraction() {
            return totalCentroids > 0 ? (double) visitedCentroids / totalCentroids : 0;
        }
        public double candidateFraction(int corpusSize) {
            return corpusSize > 0 ? (double) candidatesCollected / corpusSize : 0;
        }
    }

    public WeightedProbeStats lastProbeStats() { return lastProbeStats; }

    /**
     * Weighted scored candidates using bounded progressive dual-route IVF.
     *
     * <p>For default weights (null/all-one), delegates to the standard {@link #scoredCandidates} path.
     * For weighted queries, uses progressive probing with a hard cap at {@link #MAX_PROBE_FRACTION}
     * of total centroids:</p>
     * <ol>
     *   <li>Compute full rankings: Route A (L2/cosine) and Route B (ResonanceMoments)</li>
     *   <li>Progressive rounds: probe 15% → 25% → 40% of centroids, interleaving both routes</li>
     *   <li>In each round, collect candidates only from NEW centroids</li>
     *   <li>Early stop if a round adds fewer new candidates than topK</li>
     * </ol>
     *
     * <p>This approach bounds centroid coverage while maximizing GT centroid discovery
     * through route diversity.</p>
     */
    public ScoredCandidates scoredCandidatesWeighted(WavePattern query, int topK,
                                                      int baseNProbe,
                                                      PhaseRoutingProfile profile) {
        if (profile == null || profile.isDefault()) {
            lastProbeStats = null;
            return scoredCandidates(query, topK, baseNProbe);
        }

        CentroidIndex index = indexRef.get();
        PostingSidecar.Loaded sidecar = this.postingSidecar;
        ResonanceMoments mom = this.moments;

        if (index == null || sidecar == null) {
            lastProbeStats = null;
            return CandidateSource.super.scoredCandidates(query, topK);
        }

        int K = index.size();
        PhaseWeights pw = profile.phaseWeights();

        if (profile.isPhaseFree()) {
            lastProbeStats = new WeightedProbeStats(K, K, 0, 1, false);
            return scoredCandidates(query, topK, K);
        }

        double[] queryUDouble = UnfoldedMath.unfold(query);
        UnfoldedMath.l2Normalize(queryUDouble);
        int[] routeA = SphericalKMeans.topNearestCentroids(
                queryUDouble, index.centroids(), index.dim(), K);

        int[] routeB = (mom != null && mom.isCompatible(K, query.amplitude().length))
                ? mom.topCentroidsByMoment(query, pw, K)
                : routeA;

        if (mom != null && routeB != routeA) {
            float topScore = mom.scoreCentroid(routeB[0], query, pw);
            float midScore = mom.scoreCentroid(routeB[K / 2], query, pw);
            double spread = topScore > 0 ? (topScore - midScore) / topScore : 0;
            if (spread < MOMENT_SPREAD_THRESHOLD) {
                lastProbeStats = new WeightedProbeStats(K, K, 0, 0, false);
                return scoredCandidates(query, topK, K);
            }
        }

        int maxProbe = Math.max(baseNProbe, (int) Math.ceil(K * MAX_PROBE_FRACTION));
        maxProbe = Math.min(maxProbe, K);

        float[] queryU = UnfoldedMath.unfoldFloat32(query);
        float queryEnergy = UnfoldedMath.energyFloat32(query);

        Set<Integer> visited = new LinkedHashSet<>();
        List<PostingSidecar.ScoredCandidate> results = new ArrayList<>();
        Set<String> scoredIds = new HashSet<>();
        int roundCount = 0;
        boolean budgetExhausted = false;

        for (double roundFrac : PROBE_ROUNDS) {
            int roundTarget = Math.min(maxProbe, Math.max(baseNProbe, (int) Math.ceil(K * roundFrac)));
            roundCount++;

            int prevVisited = visited.size();
            int aIdx = 0, bIdx = 0;
            while (visited.size() < roundTarget) {
                boolean added = false;
                while (aIdx < routeA.length && visited.contains(routeA[aIdx])) aIdx++;
                if (aIdx < routeA.length && visited.size() < roundTarget) {
                    visited.add(routeA[aIdx++]);
                    added = true;
                }
                while (bIdx < routeB.length && visited.contains(routeB[bIdx])) bIdx++;
                if (bIdx < routeB.length && visited.size() < roundTarget) {
                    visited.add(routeB[bIdx++]);
                    added = true;
                }
                if (!added) break;
            }

            int newCandidates = 0;
            int idx = 0;
            for (int partIdx : visited) {
                if (idx++ < prevVisited) continue;
                int beforeSize = results.size();
                sidecar.scanPartition(partIdx, queryU, queryEnergy, results);
                newCandidates += (results.size() - beforeSize);
            }

            if (roundCount > 1 && newCandidates < topK && newCandidates < results.size() / 4) {
                break;
            }

            if (visited.size() >= maxProbe) {
                budgetExhausted = true;
                break;
            }
        }

        results.removeIf(sc -> delta.isDeleted(sc.id()));
        for (PostingSidecar.ScoredCandidate sc : results) {
            scoredIds.add(sc.id());
        }

        Map<Integer, VamanaGraph> graphs = vamanaGraphs.get();
        if (!graphs.isEmpty() && readerProvider != null) {
            for (int partIdx : visited) {
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

        lastProbeStats = new WeightedProbeStats(K, visited.size(),
                scoredIds.size(), roundCount, budgetExhausted);

        return new ScoredCandidates(results, false);
    }

    /**
     * Computes adaptive nProbe based on weight drift and phase suppression.
     *
     * <p>Uses square-root interpolation from baseNProbe to totalCentroids:
     * {@code probe = base + (K - base) * sqrt(factor)}. The square root curve
     * allocates sufficient probe budget even for small deformations (e.g. dense
     * mask with 90% active dimensions), while still scaling monotonically with
     * metric deformation.</p>
     *
     * <p>Invariant: more metric deformation → more search budget.</p>
     *
     * <p>Properties:
     * <ul>
     *   <li>all-one weights → returns baseNProbe exactly</li>
     *   <li>small drift (dense) → moderate increase (sqrt(0.1) ≈ 0.32)</li>
     *   <li>medium drift (continuous/adversarial) → substantial increase (sqrt(0.5) ≈ 0.71)</li>
     *   <li>large drift (sparse) → near-total coverage (sqrt(0.9) ≈ 0.95)</li>
     *   <li>phase-free → returns totalCentroids</li>
     * </ul>
     */
    public static int computeAdaptiveProbe(int baseNProbe, int totalCentroids, PhaseRoutingProfile profile) {
        if (profile == null || profile.isDefault()) return baseNProbe;
        if (profile.isPhaseFree()) return totalCentroids;

        double drift = profile.weightDrift();
        double suppression = 1.0 - profile.meanParticipation();

        double factor = Math.max(drift, suppression);

        int adaptiveProbe = (int) Math.ceil(
                baseNProbe + (double) (totalCentroids - baseNProbe) * Math.sqrt(factor));

        return Math.min(adaptiveProbe, totalCentroids);
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

        ResonanceMoments.Builder momentsBuilder = new ResonanceMoments.Builder(k, patternLen);

        for (String id : allValidIds) {
            if (shutdown) return;
            WavePattern p = safeReadPattern(readerProvider, idToSegment.get(id), id, patternLen);
            if (p == null) continue;
            double[] u = unfoldAndNormalize(p);
            if (u == null) continue;

            newIndex.assignNormalized(id, u);

            int nearest = SphericalKMeans.nearestCentroid(u, centroids, unfoldedDim);
            partitions.get(nearest).add(id, UnfoldedMath.unfoldFloat32(p), UnfoldedMath.energyFloat32(p));
            momentsBuilder.addPattern(nearest, p);
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

        ResonanceMoments builtMoments = momentsBuilder.build();
        Path momentsPath = this.momentsPathRef;
        if (momentsPath != null) {
            try {
                builtMoments.write(momentsPath);
            } catch (Exception e) {
                System.err.println("Failed to write moments: " + e.getMessage());
            }
        }
        this.moments = builtMoments;

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

        ResonanceMoments.Builder momentsBuilder = new ResonanceMoments.Builder(k, patternLen);

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
                momentsBuilder.addPattern(nearest, p);
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

        ResonanceMoments builtMoments = momentsBuilder.build();
        Path momentsPath = this.momentsPathRef;
        if (momentsPath != null) {
            try {
                builtMoments.write(momentsPath);
            } catch (Exception e) {
                System.err.println("Failed to write moments during sidecar rebuild: " + e.getMessage());
            }
        }
        this.moments = builtMoments;
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
