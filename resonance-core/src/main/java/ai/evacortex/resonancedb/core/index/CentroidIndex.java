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
import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class CentroidIndex {

    private final double[][] centroids;
    private final int dim;
    private final double lambda;
    private final Map<Integer, Set<String>> postings;

    public CentroidIndex(double[][] centroids, int dim, double lambda) {
        this.centroids = Objects.requireNonNull(centroids);
        this.dim = dim;
        this.lambda = lambda;
        this.postings = new ConcurrentHashMap<>(centroids.length);
        for (int i = 0; i < centroids.length; i++) {
            postings.put(i, ConcurrentHashMap.newKeySet());
        }
    }

    public void assignPattern(String patternId, WavePattern pattern) {
        double[] u = UnfoldedMath.unfold(pattern);
        UnfoldedMath.l2Normalize(u);
        assignNormalized(patternId, u);
    }

    public void assignNormalized(String patternId, double[] normalizedU) {
        double normSq = 0.0;
        for (double v : normalizedU) normSq += v * v;
        if (normSq < UnfoldedMath.NORM_SQ_FLOOR) return;

        int nearest = -1;
        double minDistSq = Double.MAX_VALUE;
        double[] distances = new double[centroids.length];

        for (int c = 0; c < centroids.length; c++) {
            double distSq = UnfoldedMath.l2DistanceSquared(normalizedU, centroids[c]);
            distances[c] = distSq;
            if (distSq < minDistSq) {
                minDistSq = distSq;
                nearest = c;
            }
        }

        double threshold = minDistSq * (1.0 + lambda) * (1.0 + lambda);
        for (int c = 0; c < centroids.length; c++) {
            if (c == nearest || distances[c] <= threshold) {
                postings.get(c).add(patternId);
            }
        }
    }

    public void removePattern(String patternId) {
        for (Set<String> posting : postings.values()) {
            posting.remove(patternId);
        }
    }

    public Set<String> queryCandidates(WavePattern query, int nProbe) {
        double[] u = UnfoldedMath.unfold(query);
        UnfoldedMath.l2Normalize(u);
        return queryCandidatesNormalized(u, nProbe);
    }

    public Set<String> queryCandidatesNormalized(double[] normalizedQueryU, int nProbe) {
        int[] topCentroids = SphericalKMeans.topNearestCentroids(
                normalizedQueryU, centroids, dim, nProbe);

        Set<String> candidates = new HashSet<>();
        for (int c : topCentroids) {
            Set<String> posting = postings.get(c);
            if (posting != null) {
                candidates.addAll(posting);
            }
        }
        return candidates;
    }

    public int size() {
        return centroids.length;
    }

    public long totalPostings() {
        long total = 0;
        for (Set<String> posting : postings.values()) {
            total += posting.size();
        }
        return total;
    }

    public int uniquePatternCount() {
        Set<String> all = new HashSet<>();
        for (Set<String> posting : postings.values()) {
            all.addAll(posting);
        }
        return all.size();
    }

    public double[][] centroids() {
        return centroids;
    }

    public int dim() {
        return dim;
    }

    public double lambda() {
        return lambda;
    }
}
