/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.math.ResonanceZone;
import ai.evacortex.resonancedb.core.math.ResonanceZoneClassifier;
import ai.evacortex.resonancedb.core.math.UnfoldedMath;
import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DeltaBuffer {

    public record Entry(String id, WavePattern pattern, Map<String, String> metadata,
                        double phaseCenter, byte[] idBytes, long lsn,
                        float[] unfoldFloat32, float energy) {}

    private final int patternLen;
    private final int unfoldedDim;

    private volatile ConcurrentHashMap<String, Entry> active = new ConcurrentHashMap<>();

    private volatile Map<String, Entry> frozen;

    private final ConcurrentHashMap<String, Long> tombstones = new ConcurrentHashMap<>();

    private static final float EXACT_MATCH_EPS = 1e-6f;

    public DeltaBuffer(int patternLen) {
        this.patternLen = patternLen;
        this.unfoldedDim = 2 * patternLen;
    }

    public void add(String id, WavePattern pattern, Map<String, String> metadata,
                    double phaseCenter, byte[] idBytes, long lsn) {
        float[] uFloat = UnfoldedMath.unfoldFloat32(pattern);
        float energy = UnfoldedMath.energyFloat32(pattern);
        Entry entry = new Entry(id, pattern, metadata, phaseCenter, idBytes, lsn,
                                uFloat, energy);
        Long tombstoneLsn = tombstones.get(id);
        if (tombstoneLsn != null && tombstoneLsn < lsn) {
            tombstones.remove(id);
        }
        active.put(id, entry);
    }

    public boolean contains(String id) {
        if (tombstones.containsKey(id)) return false;
        if (active.containsKey(id)) return true;
        Map<String, Entry> f = frozen;
        return f != null && f.containsKey(id);
    }

    public Entry remove(String id, long deleteLsn) {
        Entry removed = active.remove(id);
        if (removed != null) return removed;

        Map<String, Entry> f = frozen;
        if (f != null && f.containsKey(id)) {
            tombstones.put(id, deleteLsn);
            return f.get(id);
        }
        return null;
    }

    public void markDeleted(String id, long deleteLsn) {
        active.remove(id);
        tombstones.put(id, deleteLsn);
    }

    public Entry get(String id) {
        if (tombstones.containsKey(id)) return null;
        Entry e = active.get(id);
        if (e != null) return e;
        Map<String, Entry> f = frozen;
        return (f != null) ? f.get(id) : null;
    }

    public int size() {
        int s = active.size();
        Map<String, Entry> f = frozen;
        if (f != null) s += f.size();
        return s;
    }

    public int activeSize() {
        return active.size();
    }

    public boolean isEmpty() {
        if (!active.isEmpty()) return false;
        Map<String, Entry> f = frozen;
        return f == null || f.isEmpty();
    }

    public Map<String, Entry> freeze() {
        Map<String, Entry> snapshot = new HashMap<>(active);
        frozen = snapshot;
        active = new ConcurrentHashMap<>();
        return snapshot;
    }

    public void clearFrozen() {
        Map<String, Entry> f = frozen;
        if (f != null) {
            for (String id : f.keySet()) {
                tombstones.remove(id);
            }
        }
        frozen = null;
    }

    public ConcurrentHashMap<String, Long> tombstones() {
        return tombstones;
    }

    public List<ScoredMatch> scoreDelta(WavePattern query, String queryId,
                                        ResonanceKernel kernel, int topK) {
        List<Entry> entries = collectAllEntries();
        if (entries.isEmpty()) return List.of();

        int finalistCount = computeFinalistCount(topK, entries.size());
        if (entries.size() <= finalistCount) {
            return exactScoreAll(entries, query, queryId, kernel, topK);
        }

        int[] topIndices = selectTopFinalists(entries, query, finalistCount);
        List<ScoredMatch> results = new ArrayList<>(finalistCount);
        for (int idx : topIndices) {
            Entry e = entries.get(idx);
            float energy = kernel.compare(query, e.pattern());
            float priority = computePriority(energy, e.id(), queryId);
            results.add(new ScoredMatch(
                    new ResonanceMatch(e.id(), energy, e.pattern()), priority));
        }
        return results;
    }

    public List<ScoredMatchDetailed> scoreDeltaDetailed(WavePattern query, String queryId,
                                                         ResonanceKernel kernel, int topK) {
        List<Entry> entries = collectAllEntries();
        if (entries.isEmpty()) return List.of();

        int finalistCount = computeFinalistCount(topK, entries.size());
        if (entries.size() <= finalistCount) {
            return exactScoreAllDetailed(entries, query, queryId, kernel, topK);
        }

        int[] topIndices = selectTopFinalists(entries, query, finalistCount);
        List<ScoredMatchDetailed> results = new ArrayList<>(finalistCount);
        for (int idx : topIndices) {
            Entry e = entries.get(idx);
            ComparisonResult cr = kernel.compareWithPhaseDelta(query, e.pattern());
            float energy = cr.energy();
            double phaseShift = cr.phaseDelta();
            ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
            double zoneScore = zone.score();
            double priority = zoneScore + energy
                    + (e.id().equals(queryId) ? 1.0 : 0.0)
                    + (energy > 1.0f - EXACT_MATCH_EPS ? 0.5 : 0.0);
            results.add(new ScoredMatchDetailed(
                    new ResonanceMatchDetailed(e.id(), energy, e.pattern(), phaseShift, zone, zoneScore),
                    priority));
        }
        return results;
    }

    private int computeFinalistCount(int topK, int totalEntries) {
        int overfetch = topK <= 5 ? 4 : topK <= 10 ? 3 : 2;
        return Math.min(topK * Math.max(4, overfetch), totalEntries);
    }

    private int[] selectTopFinalists(List<Entry> entries, WavePattern query, int finalistCount) {
        float[] queryU = UnfoldedMath.unfoldFloat32(query);
        float queryEnergy = UnfoldedMath.energyFloat32(query);

        int n = entries.size();
        float[] approxScores = new float[n];
        for (int i = 0; i < n; i++) {
            Entry e = entries.get(i);
            float dot = UnfoldedMath.dotFloat32(queryU, e.unfoldFloat32(), 0, unfoldedDim);
            approxScores[i] = UnfoldedMath.scoreFromDotFloat32(dot, queryEnergy, e.energy());
        }

        int[] indices = new int[n];
        for (int i = 0; i < n; i++) indices[i] = i;
        partialSort(indices, approxScores, finalistCount);
        return Arrays.copyOf(indices, finalistCount);
    }

    private static void partialSort(int[] indices, float[] scores, int k) {
        for (int i = 0; i < k; i++) {
            int maxIdx = i;
            for (int j = i + 1; j < indices.length; j++) {
                if (scores[indices[j]] > scores[indices[maxIdx]]) {
                    maxIdx = j;
                }
            }
            int tmp = indices[i];
            indices[i] = indices[maxIdx];
            indices[maxIdx] = tmp;
        }
    }

    private static float computePriority(float energy, String id, String queryId) {
        boolean idEq = id.equals(queryId);
        boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
        return energy + (idEq ? 1.0f : 0.0f) + (exactEq ? 0.5f : 0.0f);
    }

    public record ScoredMatch(ResonanceMatch match, float priority) {}
    public record ScoredMatchDetailed(ResonanceMatchDetailed match, double priority) {}

    private List<Entry> collectAllEntries() {
        List<Entry> entries = new ArrayList<>();
        for (Entry e : active.values()) {
            if (!tombstones.containsKey(e.id())) {
                entries.add(e);
            }
        }
        Map<String, Entry> f = frozen;
        if (f != null) {
            for (Entry e : f.values()) {
                if (!tombstones.containsKey(e.id()) && !active.containsKey(e.id())) {
                    entries.add(e);
                }
            }
        }
        return entries;
    }

    private List<ScoredMatch> exactScoreAll(List<Entry> entries, WavePattern query,
                                             String queryId, ResonanceKernel kernel, int topK) {
        List<WavePattern> patterns = new ArrayList<>(entries.size());
        for (Entry e : entries) patterns.add(e.pattern());
        float[] scores = kernel.compareMany(query, patterns);

        List<ScoredMatch> results = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            float energy = scores[i];
            float priority = computePriority(energy, e.id(), queryId);
            results.add(new ScoredMatch(
                    new ResonanceMatch(e.id(), energy, e.pattern()), priority));
        }
        return results;
    }

    private List<ScoredMatchDetailed> exactScoreAllDetailed(List<Entry> entries,
                                                              WavePattern query, String queryId,
                                                              ResonanceKernel kernel, int topK) {
        List<ScoredMatchDetailed> results = new ArrayList<>(entries.size());
        for (Entry e : entries) {
            ComparisonResult cr = kernel.compareWithPhaseDelta(query, e.pattern());
            float energy = cr.energy();
            double phaseShift = cr.phaseDelta();
            ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
            double zoneScore = zone.score();
            double priority = zoneScore + energy
                    + (e.id().equals(queryId) ? 1.0 : 0.0)
                    + (energy > 1.0f - EXACT_MATCH_EPS ? 0.5 : 0.0);
            results.add(new ScoredMatchDetailed(
                    new ResonanceMatchDetailed(e.id(), energy, e.pattern(), phaseShift, zone, zoneScore),
                    priority));
        }
        return results;
    }
}
