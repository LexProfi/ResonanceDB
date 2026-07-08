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
import java.util.function.IntFunction;

public final class VamanaGraph {

    private final int[][] neighbors;
    private final String[] patternIds;
    private final int medoid;
    private final int nodeCount;
    private final int maxDegree;

    VamanaGraph(int[][] neighbors, String[] patternIds, int medoid, int maxDegree) {
        this.neighbors = neighbors;
        this.patternIds = patternIds;
        this.medoid = medoid;
        this.nodeCount = patternIds.length;
        this.maxDegree = maxDegree;
    }

    public List<String> search(WavePattern queryPattern, IntFunction<WavePattern> patternLoader,
                               int efSearch, int topK) {
        if (nodeCount == 0) return List.of();
        if (nodeCount <= efSearch) {
            return bruteForce(queryPattern, patternLoader, topK);
        }

        BitSet visited = new BitSet(nodeCount);
        PriorityQueue<NodeDist> candidates = new PriorityQueue<>(
                Comparator.comparingDouble(NodeDist::dist));
        PriorityQueue<NodeDist> results = new PriorityQueue<>(
                Comparator.comparingDouble(NodeDist::dist).reversed());

        WavePattern medoidPattern = patternLoader.apply(medoid);
        if (medoidPattern == null) return List.of();

        double medoidDist = negativeDotFused(queryPattern, medoidPattern);
        candidates.add(new NodeDist(medoid, medoidDist));
        results.add(new NodeDist(medoid, medoidDist));
        visited.set(medoid);

        while (!candidates.isEmpty()) {
            NodeDist current = candidates.poll();

            if (results.size() >= efSearch && current.dist > results.peek().dist) {
                break;
            }

            int[] nbrs = neighbors[current.node];
            if (nbrs == null) continue;

            for (int nbr : nbrs) {
                if (nbr < 0 || nbr >= nodeCount || visited.get(nbr)) continue;
                visited.set(nbr);

                WavePattern nbrPattern = patternLoader.apply(nbr);
                if (nbrPattern == null) continue;

                double dist = negativeDotFused(queryPattern, nbrPattern);

                if (results.size() < efSearch || dist < results.peek().dist) {
                    candidates.add(new NodeDist(nbr, dist));
                    results.add(new NodeDist(nbr, dist));
                    if (results.size() > efSearch) {
                        results.poll();
                    }
                }
            }
        }

        List<NodeDist> sorted = new ArrayList<>(results);
        sorted.sort(Comparator.comparingDouble(NodeDist::dist));

        List<String> ids = new ArrayList<>(Math.min(topK, sorted.size()));
        for (int i = 0; i < Math.min(topK, sorted.size()); i++) {
            ids.add(patternIds[sorted.get(i).node]);
        }
        return ids;
    }

    public List<String> search(WavePattern queryPattern, WavePattern[] patterns,
                               int efSearch, int topK) {
        return search(queryPattern, i -> (i >= 0 && i < patterns.length) ? patterns[i] : null,
                efSearch, topK);
    }

    private List<String> bruteForce(WavePattern query, IntFunction<WavePattern> loader, int topK) {
        NodeDist[] all = new NodeDist[nodeCount];
        int valid = 0;
        for (int i = 0; i < nodeCount; i++) {
            WavePattern p = loader.apply(i);
            if (p == null) continue;
            all[valid++] = new NodeDist(i, negativeDotFused(query, p));
        }
        Arrays.sort(all, 0, valid, Comparator.comparingDouble(NodeDist::dist));

        List<String> ids = new ArrayList<>(Math.min(topK, valid));
        for (int i = 0; i < Math.min(topK, valid); i++) {
            ids.add(patternIds[all[i].node]);
        }
        return ids;
    }

    static double negativeDotFused(WavePattern a, WavePattern b) {
        return -UnfoldedMath.dotFused(a, b);
    }

    public int nodeCount() { return nodeCount; }
    public int maxDegree() { return maxDegree; }
    public int medoid() { return medoid; }
    public String[] patternIds() { return patternIds; }
    public int[][] neighbors() { return neighbors; }

    record NodeDist(int node, double dist) {}
}
