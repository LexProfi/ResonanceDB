/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DeltaIndex {

    private final ConcurrentLinkedQueue<DeltaEntry> entries = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger(0);
    private final Set<String> deletedIds = ConcurrentHashMap.<String>newKeySet();
    private final ReentrantReadWriteLock drainLock = new ReentrantReadWriteLock();

    public record DeltaEntry(String patternId, String segmentName) {}

    public void add(String patternId, String segmentName) {
        drainLock.readLock().lock();
        try {
            entries.add(new DeltaEntry(patternId, segmentName));
            size.incrementAndGet();
        } finally {
            drainLock.readLock().unlock();
        }
    }

    public void markDeleted(String patternId) {
        deletedIds.add(patternId);
    }

    public Map<String, Collection<String>> candidatesBySegment() {
        drainLock.readLock().lock();
        try {
            Map<String, Collection<String>> result = new LinkedHashMap<>();
            for (DeltaEntry entry : entries) {
                if (!deletedIds.contains(entry.patternId)) {
                    result.computeIfAbsent(entry.segmentName, k -> new ArrayList<>())
                            .add(entry.patternId);
                }
            }
            return result;
        } finally {
            drainLock.readLock().unlock();
        }
    }

    public Set<String> allIds() {
        drainLock.readLock().lock();
        try {
            Set<String> ids = new HashSet<>();
            for (DeltaEntry entry : entries) {
                if (!deletedIds.contains(entry.patternId)) {
                    ids.add(entry.patternId);
                }
            }
            return ids;
        } finally {
            drainLock.readLock().unlock();
        }
    }

    public int size() {
        return size.get();
    }

    public boolean exceedsThreshold(int maxSize) {
        return size.get() > maxSize;
    }

    public boolean exceedsFraction(int totalPatterns, double fraction) {
        return size.get() > totalPatterns * fraction;
    }

    public boolean isDeleted(String patternId) {
        return deletedIds.contains(patternId);
    }

    public List<DeltaEntry> drainAll() {
        drainLock.writeLock().lock();
        try {
            List<DeltaEntry> drained = new ArrayList<>(entries);
            entries.clear();
            deletedIds.clear();
            size.set(0);
            return drained;
        } finally {
            drainLock.writeLock().unlock();
        }
    }
}
