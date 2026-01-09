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

    private static final double PI = Math.PI;
    private static final double TWO_PI = 2 * Math.PI;

    private final NavigableMap<Double, String> phaseShardMap;
    private final double epsilon;
    private final int totalShards;
    private final boolean useExplicitRanges;

    public PhaseShardSelector(Map<Double, String> phaseShardMap, double epsilon) {
        if (phaseShardMap == null || phaseShardMap.isEmpty())
            throw new IllegalArgumentException("Phase shard map must not be null or empty.");

        this.phaseShardMap = new ConcurrentSkipListMap<>();
        for (Map.Entry<Double, String> e : phaseShardMap.entrySet()) {
            double k = normalizePhase(e.getKey());
            while (this.phaseShardMap.containsKey(k)) {
                k = Math.nextUp(k);
            }
            this.phaseShardMap.put(k, e.getValue());
        }
        this.epsilon = Math.max(0.0, epsilon);
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
            int shardIndex = Math.floorMod(Double.hashCode(meanPhase), totalShards);
            return "phase-" + shardIndex + ".segment";
        }
    }

    private String routeToShard(WavePattern pattern) {
        double avgPhase = normalizePhase(Arrays.stream(pattern.phase()).average().orElse(0.0));
        Map.Entry<Double, String> entry = phaseShardMap.floorEntry(avgPhase);
        if (entry != null) return entry.getValue();
        return phaseShardMap.firstEntry().getValue();
    }

    public List<String> getRelevantShards(WavePattern query) {
        return getRelevantShards(query, this.epsilon);
    }

    public double[] getPhaseRange(WavePattern pattern) {
        double avgPhase = normalizePhase(Arrays.stream(pattern.phase()).average().orElse(0.0));
        if (useExplicitRanges) {
            return new double[]{ avgPhase - epsilon, avgPhase + epsilon };
        } else {
            return new double[]{ avgPhase, avgPhase };
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
        return Collections.unmodifiableList(new ArrayList<>(phaseShardMap.values()));
    }

    public static PhaseShardSelector fromManifest(Collection<ManifestIndex.PatternLocation> locations, double epsilon) {
        TreeMap<Double, String> map = new TreeMap<>();
        Map<String, double[]> accum = new HashMap<>();
        for (ManifestIndex.PatternLocation loc : locations) {
            accum.compute(loc.segmentName(), (k, v) -> {
                double p = normalizePhase(loc.phaseCenter());
                if (v == null) return new double[]{p, 1.0};
                v[0] += p; v[1] += 1.0; return v;
            });
        }
        for (Map.Entry<String, double[]> e : accum.entrySet()) {
            String segment = e.getKey();
            double avg = e.getValue()[0] / Math.max(1.0, e.getValue()[1]);
            avg = normalizePhase(avg);
            while (map.containsKey(avg)) avg = Math.nextUp(avg);
            map.put(avg, segment);
        }
        return new PhaseShardSelector(map, epsilon);
    }

    public static PhaseShardSelector emptyFallback() {
        return new PhaseShardSelector(Map.of(0.0, "phase-0.segment"), Math.PI);
    }

    public String fallbackRouteIfLowCoherence(WavePattern query) {
        return null;
    }

    public List<String> orderedShardList() {
        if (!useExplicitRanges) {
            List<String> list = new ArrayList<>(totalShards);
            for (int i = 0; i < totalShards; i++) list.add("phase-" + i + ".segment");
            return Collections.unmodifiableList(list);
        }
        return Collections.unmodifiableList(new ArrayList<>(phaseShardMap.values()));
    }

    public int indexOf(String segmentName) {
        List<String> ord = orderedShardList();
        for (int i = 0; i < ord.size(); i++) if (ord.get(i).equals(segmentName)) return i;
        return -1;
    }

    public List<String> getRelevantShards(WavePattern query, double customEps) {
        if (!useExplicitRanges) {
            return List.of(selectShard(query));
        }
        if (phaseShardMap.isEmpty()) return List.of();

        double avg = normalizePhase(Arrays.stream(query.phase()).average().orElse(0.0));
        double eps = Math.max(0.0, customEps);

        double min = avg - eps;
        double max = avg + eps;

        List<String> out = new ArrayList<>();
        if (min < -PI) {
            out.addAll(phaseShardMap.subMap(-PI, true, clamp(max, -PI, PI), true).values());
            out.addAll(phaseShardMap.subMap(clamp(min + TWO_PI, -PI, PI), true, PI, true).values());
        } else if (max > PI) {
            out.addAll(phaseShardMap.subMap(clamp(min, -PI, PI), true, PI, true).values());
            out.addAll(phaseShardMap.subMap(-PI, true, clamp(max - TWO_PI, -PI, PI), true).values());
        } else {
            out.addAll(phaseShardMap.subMap(clamp(min, -PI, PI), true, clamp(max, -PI, PI), true).values());
        }

        if (out.isEmpty()) {
            return new ArrayList<>(phaseShardMap.values());
        }
        return new ArrayList<>(new LinkedHashSet<>(out));
    }

    private static double normalizePhase(double x) {
        double y = x;
        while (y <= -PI) y += TWO_PI;
        while (y >   PI) y -= TWO_PI;
        return y;
    }

    private static double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(hi, v));
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