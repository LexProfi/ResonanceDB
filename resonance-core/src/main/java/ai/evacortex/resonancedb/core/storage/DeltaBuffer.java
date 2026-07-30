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
import ai.evacortex.resonancedb.core.math.UnfoldedMath;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DeltaBuffer {

    public record Entry(String id, WavePattern pattern, Map<String, String> metadata,
                        double phaseCenter, byte[] idBytes, long lsn,
                        float[] unfoldFloat32, float energy) {}

    private volatile ConcurrentHashMap<String, Entry> active = new ConcurrentHashMap<>();

    private volatile Map<String, Entry> frozen;

    private final ConcurrentHashMap<String, Long> tombstones = new ConcurrentHashMap<>();

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

    public record ScoredMatch(ResonanceMatch match) {}

    private static final Comparator<ScoredMatch> SCORED_ORDER = Comparator
            .comparingDouble((ScoredMatch sm) -> sm.match().energy())
            .thenComparing(sm -> sm.match().id(), Comparator.reverseOrder());

    public List<ScoredMatch> scoreDelta(WavePattern query, String queryId,
                                        ResonanceKernel kernel, int topK) {
        List<Entry> entries = collectAllEntries();
        if (entries.isEmpty()) return List.of();

        PriorityQueue<ScoredMatch> heap = new PriorityQueue<>(Math.max(topK, 8), SCORED_ORDER);

        List<WavePattern> patterns = new ArrayList<>(entries.size());
        for (Entry e : entries) patterns.add(e.pattern());
        float[] scores = kernel.compareMany(query, patterns);

        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            float energy = scores[i];
            ScoredMatch sm = new ScoredMatch(new ResonanceMatch(e.id(), energy, e.pattern()));
            if (heap.size() < topK) {
                heap.add(sm);
            } else if (SCORED_ORDER.compare(sm, heap.peek()) > 0) {
                heap.poll();
                heap.add(sm);
            }
        }
        return new ArrayList<>(heap);
    }

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
}
