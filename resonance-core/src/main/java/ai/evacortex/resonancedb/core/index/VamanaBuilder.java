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
import java.util.function.BooleanSupplier;

public final class VamanaBuilder {

    public static final int DEFAULT_R = 32;
    public static final int DEFAULT_L_BUILD = 75;
    public static final double DEFAULT_ALPHA = 1.2;

    private VamanaBuilder() {}

    public static VamanaGraph build(String[] patternIds, WavePattern[] patterns,
                                     int R, int Lbuild, double alpha, long seed) {
        return build(patternIds, patterns, R, Lbuild, alpha, seed, () -> false);
    }

    public static VamanaGraph build(String[] patternIds, WavePattern[] patterns,
                                     int R, int Lbuild, double alpha, long seed,
                                     BooleanSupplier cancelled) {
        int n = patternIds.length;
        if (n == 0) {
            return new VamanaGraph(new int[0][], new String[0], 0, R);
        }
        if (n == 1) {
            return new VamanaGraph(new int[][]{ new int[0] }, patternIds.clone(), 0, R);
        }

        int medoid = findMedoid(patterns, n, seed);

        @SuppressWarnings("unchecked")
        List<Integer>[] adj = new List[n];
        for (int i = 0; i < n; i++) {
            adj[i] = new ArrayList<>(R);
        }

        int[] order = new int[n];
        for (int i = 0; i < n; i++) order[i] = i;
        Random rng = new Random(seed);
        for (int i = n - 1; i > 0; i--) {
            int j = rng.nextInt(i + 1);
            int tmp = order[i]; order[i] = order[j]; order[j] = tmp;
        }

        for (int idx : order) {
            if (cancelled.getAsBoolean()) {
                return new VamanaGraph(new int[0][], new String[0], -1, R);
            }

            List<VamanaGraph.NodeDist> candidates = greedySearch(
                    patterns[idx], patterns, adj, medoid, Lbuild, n);

            List<Integer> pruned = robustPrune(idx, candidates, patterns, alpha, R);
            adj[idx] = pruned;

            for (int nbr : pruned) {
                if (!adj[nbr].contains(idx)) {
                    adj[nbr].add(idx);
                    if (adj[nbr].size() > R) {
                        List<VamanaGraph.NodeDist> nbrCands = new ArrayList<>();
                        for (int k : adj[nbr]) {
                            nbrCands.add(new VamanaGraph.NodeDist(k,
                                    VamanaGraph.negativeDotFused(patterns[nbr], patterns[k])));
                        }
                        adj[nbr] = robustPrune(nbr, nbrCands, patterns, alpha, R);
                    }
                }
            }
        }

        int[][] neighbors = new int[n][];
        for (int i = 0; i < n; i++) {
            List<Integer> list = adj[i];
            neighbors[i] = new int[list.size()];
            for (int j = 0; j < list.size(); j++) {
                neighbors[i][j] = list.get(j);
            }
        }

        return new VamanaGraph(neighbors, patternIds.clone(), medoid, R);
    }

    public static VamanaGraph build(String[] patternIds, WavePattern[] patterns, long seed) {
        return build(patternIds, patterns, DEFAULT_R, DEFAULT_L_BUILD, DEFAULT_ALPHA, seed);
    }

    private static List<VamanaGraph.NodeDist> greedySearch(
            WavePattern query, WavePattern[] patterns,
            List<Integer>[] adj, int entryPoint, int Lbuild, int n) {

        BitSet visited = new BitSet(n);
        PriorityQueue<VamanaGraph.NodeDist> candidates = new PriorityQueue<>(
                Comparator.comparingDouble(VamanaGraph.NodeDist::dist));
        PriorityQueue<VamanaGraph.NodeDist> results = new PriorityQueue<>(
                Comparator.comparingDouble(VamanaGraph.NodeDist::dist).reversed());

        double entryDist = VamanaGraph.negativeDotFused(query, patterns[entryPoint]);
        candidates.add(new VamanaGraph.NodeDist(entryPoint, entryDist));
        results.add(new VamanaGraph.NodeDist(entryPoint, entryDist));
        visited.set(entryPoint);

        while (!candidates.isEmpty()) {
            VamanaGraph.NodeDist current = candidates.poll();

            if (results.size() >= Lbuild && current.dist() > results.peek().dist()) {
                break;
            }

            List<Integer> nbrs = adj[current.node()];
            for (int nbr : nbrs) {
                if (visited.get(nbr)) continue;
                visited.set(nbr);

                double dist = VamanaGraph.negativeDotFused(query, patterns[nbr]);

                if (results.size() < Lbuild || dist < results.peek().dist()) {
                    candidates.add(new VamanaGraph.NodeDist(nbr, dist));
                    results.add(new VamanaGraph.NodeDist(nbr, dist));
                    if (results.size() > Lbuild) {
                        results.poll();
                    }
                }
            }
        }

        return new ArrayList<>(results);
    }

    private static List<Integer> robustPrune(int node,
                                              List<VamanaGraph.NodeDist> candidates,
                                              WavePattern[] patterns,
                                              double alpha, int R) {
        candidates.sort(Comparator.comparingDouble(VamanaGraph.NodeDist::dist));

        candidates.removeIf(nd -> nd.node() == node);

        List<Integer> selected = new ArrayList<>(R);

        for (VamanaGraph.NodeDist candidate : candidates) {
            if (selected.size() >= R) break;

            boolean pruned = false;
            for (int sel : selected) {
                double distSelCand = VamanaGraph.negativeDotFused(
                        patterns[sel], patterns[candidate.node()]);
                if (distSelCand <= alpha * candidate.dist()) {
                    pruned = true;
                    break;
                }
            }

            if (!pruned) {
                selected.add(candidate.node());
            }
        }

        return selected;
    }

    private static int findMedoid(WavePattern[] patterns, int n, long seed) {
        if (n <= 100) {
            return findMedoidExact(patterns, n);
        }

        Random rng = new Random(seed + 31);
        int sampleSize = Math.min(n, (int) Math.ceil(Math.sqrt(n)));
        int dim = patterns[0].amplitude().length;
        int uDim = dim * 2;

        double[] meanU = new double[uDim];
        for (int s = 0; s < sampleSize; s++) {
            int idx = rng.nextInt(n);
            double[] u = UnfoldedMath.unfold(patterns[idx]);
            for (int d = 0; d < uDim; d++) {
                meanU[d] += u[d];
            }
        }
        for (int d = 0; d < uDim; d++) {
            meanU[d] /= sampleSize;
        }
        UnfoldedMath.l2Normalize(meanU);

        int best = 0;
        double bestDot = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < n; i++) {
            double[] u = UnfoldedMath.unfold(patterns[i]);
            UnfoldedMath.l2Normalize(u);
            double dot = UnfoldedMath.dotUnfolded(u, meanU);
            if (dot > bestDot) {
                bestDot = dot;
                best = i;
            }
        }
        return best;
    }

    private static int findMedoidExact(WavePattern[] patterns, int n) {
        double[] totalDist = new double[n];
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                double d = VamanaGraph.negativeDotFused(patterns[i], patterns[j]);
                totalDist[i] += d;
                totalDist[j] += d;
            }
        }
        int best = 0;
        for (int i = 1; i < n; i++) {
            if (totalDist[i] < totalDist[best]) best = i;
        }
        return best;
    }
}
