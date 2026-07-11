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

    private volatile ConcurrentHashMap<String, Entry> active = new ConcurrentHashMap<>();

    private volatile Map<String, Entry> frozen;

    private final ConcurrentHashMap<String, Long> tombstones = new ConcurrentHashMap<>();

    private static final float EXACT_MATCH_EPS = 1e-6f;

    public DeltaBuffer(int patternLen) {
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
        return exactScoreAll(entries, query, queryId, kernel, topK);
    }

    public List<ScoredMatchDetailed> scoreDeltaDetailed(WavePattern query, String queryId,
                                                         ResonanceKernel kernel, int topK) {
        List<Entry> entries = collectAllEntries();
        if (entries.isEmpty()) return List.of();
        return exactScoreAllDetailed(entries, query, queryId, kernel, topK);
    }

    public record ScoredMatch(ResonanceMatch match, ResonanceZone zone,
                              boolean idMatch, boolean exactMatch) {}
    public record ScoredMatchDetailed(ResonanceMatchDetailed match,
                                      boolean idMatch, boolean exactMatch) {}

    private static final Comparator<ScoredMatch> SCORED_ORDER = Comparator
            .comparing(ScoredMatch::idMatch)
            .thenComparing(ScoredMatch::exactMatch)
            .thenComparing(ScoredMatch::zone)
            .thenComparingDouble(sm -> sm.match().energy())
            .thenComparing(sm -> sm.match().id(), Comparator.reverseOrder());

    private static final Comparator<ScoredMatchDetailed> SCORED_DETAILED_ORDER = Comparator
            .comparing(ScoredMatchDetailed::idMatch)
            .thenComparing(ScoredMatchDetailed::exactMatch)
            .thenComparing(sm -> sm.match().zone())
            .thenComparingDouble(sm -> sm.match().energy())
            .thenComparing(sm -> sm.match().id(), Comparator.reverseOrder());

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
        PriorityQueue<ScoredMatch> heap = new PriorityQueue<>(Math.max(topK, 8), SCORED_ORDER);

        for (Entry e : entries) {
            ComparisonResult result = kernel.compareWithPhaseDelta(query, e.pattern());
            float energy = result.energy();
            ResonanceZone zone = ResonanceZoneClassifier.classify(energy, result.phaseDelta());
            boolean idMatch = e.id().equals(queryId);
            boolean exactMatch = energy > 1.0f - EXACT_MATCH_EPS;
            ScoredMatch sm = new ScoredMatch(
                    new ResonanceMatch(e.id(), energy, e.pattern()), zone, idMatch, exactMatch);
            if (heap.size() < topK) {
                heap.add(sm);
            } else if (SCORED_ORDER.compare(sm, heap.peek()) > 0) {
                heap.poll();
                heap.add(sm);
            }
        }
        return new ArrayList<>(heap);
    }

    private List<ScoredMatchDetailed> exactScoreAllDetailed(List<Entry> entries,
                                                              WavePattern query, String queryId,
                                                              ResonanceKernel kernel, int topK) {
        PriorityQueue<ScoredMatchDetailed> heap = new PriorityQueue<>(Math.max(topK, 8), SCORED_DETAILED_ORDER);

        for (Entry e : entries) {
            ComparisonResult cr = kernel.compareWithPhaseDelta(query, e.pattern());
            float energy = cr.energy();
            double phaseShift = cr.phaseDelta();
            ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
            double zoneScore = zone.score();
            boolean idMatch = e.id().equals(queryId);
            boolean exactMatch = energy > 1.0f - EXACT_MATCH_EPS;
            ScoredMatchDetailed smd = new ScoredMatchDetailed(
                    new ResonanceMatchDetailed(e.id(), energy, e.pattern(), phaseShift, zone, zoneScore),
                    idMatch, exactMatch);
            if (heap.size() < topK) {
                heap.add(smd);
            } else if (SCORED_DETAILED_ORDER.compare(smd, heap.peek()) > 0) {
                heap.poll();
                heap.add(smd);
            }
        }
        return new ArrayList<>(heap);
    }
}
