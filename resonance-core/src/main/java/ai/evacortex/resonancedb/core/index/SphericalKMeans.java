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

import java.util.Arrays;
import java.util.Random;

public final class SphericalKMeans {

    private static final int DEFAULT_MAX_ITERATIONS = 25;
    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final double CONVERGENCE_THRESHOLD = 1e-6;

    private SphericalKMeans() {}

    public static double[][] fit(double[][] data, int k, int dim, long seed) {
        return fit(data, k, dim, seed, DEFAULT_MAX_ITERATIONS, DEFAULT_BATCH_SIZE);
    }

    public static double[][] fit(double[][] data, int k, int dim, long seed,
                                  int maxIterations, int batchSize) {
        if (data.length == 0) {
            throw new IllegalArgumentException("Empty data");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive: " + k);
        }
        if (k > data.length) {
            k = data.length;
        }

        Random rng = new Random(seed);
        double[][] centroids = initPlusPlus(data, k, dim, rng);
        int effectiveBatch = (batchSize <= 0 || batchSize >= data.length) ? data.length : batchSize;

        int[] assignments = new int[effectiveBatch];
        int[] clusterSizes = new int[k];

        for (int iter = 0; iter < maxIterations; iter++) {
            int[] batchIndices = selectBatch(data.length, effectiveBatch, rng);

            Arrays.fill(clusterSizes, 0);
            for (int b = 0; b < batchIndices.length; b++) {
                double[] point = data[batchIndices[b]];
                int nearest = nearestCentroid(point, centroids, dim);
                assignments[b] = nearest;
                clusterSizes[nearest]++;
            }

            double[][] newCentroids = new double[k][dim];
            for (int b = 0; b < batchIndices.length; b++) {
                double[] point = data[batchIndices[b]];
                int cluster = assignments[b];
                for (int d = 0; d < dim; d++) {
                    newCentroids[cluster][d] += point[d];
                }
            }

            double maxDrift = 0.0;
            for (int c = 0; c < k; c++) {
                if (clusterSizes[c] == 0) {
                    int randIdx = rng.nextInt(data.length);
                    System.arraycopy(data[randIdx], 0, newCentroids[c], 0, dim);
                }
                UnfoldedMath.l2Normalize(newCentroids[c]);
                double drift = UnfoldedMath.l2DistanceSquared(centroids[c], newCentroids[c]);
                maxDrift = Math.max(maxDrift, drift);
            }

            centroids = newCentroids;

            if (maxDrift < CONVERGENCE_THRESHOLD) {
                break;
            }
        }

        return centroids;
    }

    public static int recommendedK(int n) {
        int k = (int) Math.ceil(Math.sqrt(n));
        return Math.max(16, Math.min(k, n));
    }

    static int nearestCentroid(double[] point, double[][] centroids, int dim) {
        int best = 0;
        double bestDot = Double.NEGATIVE_INFINITY;
        for (int c = 0; c < centroids.length; c++) {
            double dot = 0.0;
            for (int d = 0; d < dim; d++) {
                dot += point[d] * centroids[c][d];
            }
            if (dot > bestDot) {
                bestDot = dot;
                best = c;
            }
        }
        return best;
    }

    static int[] topNearestCentroids(double[] point, double[][] centroids, int dim, int nProbe) {
        int k = centroids.length;
        nProbe = Math.max(0, Math.min(nProbe, k));
        if (nProbe == 0) return new int[0];

        double[] dots = new double[k];
        for (int c = 0; c < k; c++) {
            double dot = 0.0;
            for (int d = 0; d < dim; d++) {
                dot += point[d] * centroids[c][d];
            }
            dots[c] = dot;
        }

        int[] indices = new int[k];
        for (int i = 0; i < k; i++) indices[i] = i;

        for (int i = 0; i < nProbe; i++) {
            int maxIdx = i;
            for (int j = i + 1; j < k; j++) {
                if (dots[indices[j]] > dots[indices[maxIdx]]) {
                    maxIdx = j;
                }
            }
            int tmp = indices[i];
            indices[i] = indices[maxIdx];
            indices[maxIdx] = tmp;
        }

        return Arrays.copyOf(indices, nProbe);
    }

    private static double[][] initPlusPlus(double[][] data, int k, int dim, Random rng) {
        double[][] centroids = new double[k][dim];

        int first = rng.nextInt(data.length);
        System.arraycopy(data[first], 0, centroids[0], 0, dim);

        double[] minDist = new double[data.length];
        Arrays.fill(minDist, Double.MAX_VALUE);

        for (int c = 1; c < k; c++) {
            double totalWeight = 0.0;
            for (int i = 0; i < data.length; i++) {
                double dist = UnfoldedMath.l2DistanceSquared(data[i], centroids[c - 1]);
                minDist[i] = Math.min(minDist[i], dist);
                totalWeight += minDist[i];
            }

            if (totalWeight <= 0.0) {
                for (int fill = c; fill < k; fill++) {
                    System.arraycopy(data[rng.nextInt(data.length)], 0, centroids[fill], 0, dim);
                    UnfoldedMath.l2Normalize(centroids[fill]);
                }
                break;
            }

            double r = rng.nextDouble() * totalWeight;
            double cumulative = 0.0;
            int selected = data.length - 1;
            for (int i = 0; i < data.length; i++) {
                cumulative += minDist[i];
                if (cumulative >= r) {
                    selected = i;
                    break;
                }
            }

            System.arraycopy(data[selected], 0, centroids[c], 0, dim);
            UnfoldedMath.l2Normalize(centroids[c]);
        }

        return centroids;
    }

    private static int[] selectBatch(int total, int batchSize, Random rng) {
        if (batchSize >= total) {
            int[] all = new int[total];
            for (int i = 0; i < total; i++) all[i] = i;
            return all;
        }

        int[] indices = new int[total];
        for (int i = 0; i < total; i++) indices[i] = i;

        for (int i = 0; i < batchSize; i++) {
            int j = i + rng.nextInt(total - i);
            int tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }

        return Arrays.copyOf(indices, batchSize);
    }
}
