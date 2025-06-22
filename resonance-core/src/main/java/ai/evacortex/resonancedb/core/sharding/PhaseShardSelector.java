/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.sharding;

import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class PhaseShardSelector {

    private final NavigableMap<Double, String> phaseShardMap;
    private final double epsilon;
    private final int totalShards;
    private final boolean useExplicitRanges;

    public PhaseShardSelector(Map<Double, String> phaseShardMap, double epsilon) {
        if (phaseShardMap == null || phaseShardMap.isEmpty())
            throw new IllegalArgumentException("Phase shard map must not be null or empty.");

        this.phaseShardMap = new ConcurrentSkipListMap<>(phaseShardMap);
        this.epsilon = epsilon;
        this.totalShards = phaseShardMap.size();
        this.useExplicitRanges = true;
    }

    public PhaseShardSelector(int totalShards) {
        if (totalShards <= 0)
            throw new IllegalArgumentException("Shard count must be > 0");

        this.phaseShardMap = null;
        this.epsilon = 0.0;
        this.totalShards = totalShards;
        this.useExplicitRanges = false;
    }

    public String selectShard(WavePattern pattern) {
        if (useExplicitRanges) {
            return routeToShard(pattern);
        } else {
            double meanPhase = Arrays.stream(pattern.phase()).average().orElse(0.0);
            int shardIndex = Math.floorMod(Double.hashCode(meanPhase * 1000), totalShards);
            return "phase-" + shardIndex + ".segment";
        }
    }

    private String routeToShard(WavePattern pattern) {
        double avgPhase = Arrays.stream(pattern.phase()).average().orElse(0.0);
        Map.Entry<Double, String> entry = phaseShardMap.floorEntry(avgPhase);
        if (entry == null) return phaseShardMap.firstEntry().getValue();
        return entry.getValue();
    }

    public List<String> getRelevantShards(WavePattern query) {
        return getRelevantShards(query, this.epsilon);      // ✨ changed
    }

    public double[] getPhaseRange(WavePattern pattern) {
        double avgPhase = Arrays.stream(pattern.phase()).average().orElse(0.0);
        if (useExplicitRanges) {
            return new double[] { avgPhase - epsilon, avgPhase + epsilon };
        } else {
            return new double[] { avgPhase, avgPhase };
        }
    }

    public List<String> allShards() {
        if (!useExplicitRanges) {
            List<String> result = new ArrayList<>(totalShards);
            for (int i = 0; i < totalShards; i++) {
                result.add("phase-" + i + ".segment");
            }
            return Collections.unmodifiableList(result);
        }
        return new ArrayList<>(phaseShardMap.values());
    }


    public static PhaseShardSelector fromManifest(Collection<ManifestIndex.PatternLocation> locations, double epsilon) {
        Map<String, List<Double>> grouped = new HashMap<>();
        for (ManifestIndex.PatternLocation loc : locations) {
            grouped.computeIfAbsent(loc.segmentName(), k -> new ArrayList<>()).add(loc.phaseCenter());
        }

        TreeMap<Double, String> map = new TreeMap<>();
        for (Map.Entry<String, List<Double>> entry : grouped.entrySet()) {
            String segment = entry.getKey();
            List<Double> phases = entry.getValue();
            double avg = phases.stream().mapToDouble(d -> d).average().orElse(0.0);
            map.put(avg, segment);
        }

        return new PhaseShardSelector(map, epsilon);
    }

    public static PhaseShardSelector emptyFallback() {
        return new PhaseShardSelector(Map.of(0.0, "phase-0.segment"), Math.PI);
    }

    public String fallbackRouteIfLowCoherence(WavePattern query) {
        // TODO: implement phase-degrading route fallback logic
        return null;
    }

    // ✨ new — упорядоченный (по mean φ) неизменяемый список имён сегментов
    public List<String> orderedShardList() {
        if (!useExplicitRanges) {
            List<String> list = new ArrayList<>(totalShards);
            for (int i = 0; i < totalShards; i++) list.add("phase-" + i + ".segment");
            return Collections.unmodifiableList(list);
        }
        return Collections.unmodifiableList(new ArrayList<>(phaseShardMap.values()));
    }

    // ✨ new — индекс сегмента в orderedShardList(), -1 если не найден
    public int indexOf(String segmentName) {
        List<String> ord = orderedShardList();
        for (int i = 0; i < ord.size(); i++) {
            if (ord.get(i).equals(segmentName)) return i;
        }
        return -1;
    }

    // ✨ new — перегрузка с произвольным ε (используется ripple-алгоритмом)
    public List<String> getRelevantShards(WavePattern query, double customEps) {
        if (!useExplicitRanges) {
            return List.of(selectShard(query));
        }
        double avg = Arrays.stream(query.phase()).average().orElse(0.0);
        double min = avg - customEps;
        double max = avg + customEps;
        Collection<String> hits = phaseShardMap.subMap(min, true, max, true).values();
        return new ArrayList<>(hits.isEmpty() ? phaseShardMap.values() : hits);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PhaseShardSelector that)) return false;
        return totalShards == that.totalShards &&
                Double.compare(that.epsilon, epsilon) == 0 &&
                Objects.equals(phaseShardMap, that.phaseShardMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phaseShardMap, epsilon, totalShards);
    }
}