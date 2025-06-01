/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.sharding;

import ai.evacortex.resonancedb.core.WavePattern;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * PhaseShardSelector determines the target segment name based on the phase information of a WavePattern.
 * Supports both explicit range-based sharding and uniform hash-based sharding.
 */
public class PhaseShardSelector {

    private final NavigableMap<Double, String> phaseShardMap;
    private final double epsilon;
    private final int totalShards;
    private final boolean useExplicitRanges;

    /**
     * Constructs a range-based selector using explicit phase→shard mapping.
     * @param phaseShardMap key: center of phase range, value: segment name
     * @param epsilon half-width of acceptable phase deviation
     */
    public PhaseShardSelector(Map<Double, String> phaseShardMap, double epsilon) {
        if (phaseShardMap == null || phaseShardMap.isEmpty())
            throw new IllegalArgumentException("Phase shard map must not be null or empty.");

        this.phaseShardMap = new ConcurrentSkipListMap<>(phaseShardMap);
        this.epsilon = epsilon;
        this.totalShards = phaseShardMap.size();
        this.useExplicitRanges = true;
    }

    /**
     * Constructs a uniform hash-based sharding selector.
     * @param totalShards number of uniformly distributed shards
     */
    public PhaseShardSelector(int totalShards) {
        if (totalShards <= 0)
            throw new IllegalArgumentException("Shard count must be > 0");

        this.phaseShardMap = null;
        this.epsilon = 0.0;
        this.totalShards = totalShards;
        this.useExplicitRanges = false;
    }

    /**
     * Selects the target shard name for a given pattern.
     */
    public String selectShard(WavePattern pattern) {
        if (useExplicitRanges) {
            return routeToShard(pattern);
        } else {
            double meanPhase = Arrays.stream(pattern.phase()).average().orElse(0.0);
            int shardIndex = Math.floorMod(Double.hashCode(meanPhase * 1000), totalShards);
            return "phase-" + shardIndex + ".segment";
        }
    }

    /**
     * Explicit range-based routing using nearest center.
     */
    private String routeToShard(WavePattern pattern) {
        double avgPhase = Arrays.stream(pattern.phase()).average().orElse(0.0);
        Map.Entry<Double, String> entry = phaseShardMap.floorEntry(avgPhase);
        if (entry == null) return phaseShardMap.firstEntry().getValue();
        return entry.getValue();
    }

    /**
     * Returns a list of segment names relevant to the query pattern within epsilon range.
     */
    public List<String> getRelevantShards(WavePattern query) {
        if (!useExplicitRanges) {
            return List.of(selectShard(query));
        }

        double avgPhase = Arrays.stream(query.phase()).average().orElse(0.0);
        double min = avgPhase - epsilon;
        double max = avgPhase + epsilon;

        return new ArrayList<>(phaseShardMap.subMap(min, true, max, true).values());
    }

    /**
     * Returns the (min, max) phase range used for querying.
     */
    public double[] getPhaseRange(WavePattern pattern) {
        double avgPhase = Arrays.stream(pattern.phase()).average().orElse(0.0);
        if (useExplicitRanges) {
            return new double[] { avgPhase - epsilon, avgPhase + epsilon };
        } else {
            return new double[] { avgPhase, avgPhase };
        }
    }

    /**
     * Returns all known shard segment names.
     */
    public List<String> allShards() {
        if (!useExplicitRanges) {
            return Collections.unmodifiableList(
                    new ArrayList<>() {{
                        for (int i = 0; i < totalShards; i++) {
                            add("phase-" + i + ".segment");
                        }
                    }}
            );
        }
        return new ArrayList<>(phaseShardMap.values());
    }

    /**
     * Builds a PhaseShardSelector using ManifestIndex data.
     * Assumes range-based routing by clustering phase centers.
     */
    public static PhaseShardSelector fromManifest(Collection<ManifestIndex.PatternLocation> locations, double epsilon) {
        TreeMap<Double, String> map = new TreeMap<>();
        for (ManifestIndex.PatternLocation loc : locations) {
            map.put(loc.phaseCenter(), loc.segmentName());
        }
        return new PhaseShardSelector(map, epsilon);
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