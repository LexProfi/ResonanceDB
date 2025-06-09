/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.*;
import ai.evacortex.resonancedb.core.engine.ResonanceEngine;
import ai.evacortex.resonancedb.core.math.ResonanceZone;
import ai.evacortex.resonancedb.core.math.ResonanceZoneClassifier;
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.exceptions.SegmentOverflowException;
import ai.evacortex.resonancedb.core.math.WavePatternUtils;
import ai.evacortex.resonancedb.core.metadata.PatternMetaStore;
import ai.evacortex.resonancedb.core.sharding.PhaseShardSelector;
import ai.evacortex.resonancedb.core.storage.io.SegmentReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import ai.evacortex.resonancedb.core.storage.responce.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * WavePatternStoreImpl is the main implementation of the ResonanceStore interface,
 * responsible for managing insertions, deletions, updates, and queries of WavePatterns,
 * with support for phase-based sharding using a PhaseShardSelector.
 */
public class WavePatternStoreImpl implements ResonanceStore, Closeable {

    private static final double READ_EPSILON  = 0.1;
    private static final float EXACT_MATCH_EPS = 1e-6f;
    private final ManifestIndex manifest;
    private final PatternMetaStore metaStore;
    private final Map<String, SegmentWriter> segmentWriters;
    private final Path rootDir;
    private final AtomicReference<PhaseShardSelector> shardSelectorRef;
    private final ResonanceTracer tracer;
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();
    private record HeapItem(ResonanceMatch match, float priority) {}
    private record HeapItemDetailed(ResonanceMatchDetailed match, double priority) {}

    public WavePatternStoreImpl(Path dbRoot) {
        this.rootDir = dbRoot;
        this.manifest = ManifestIndex.loadOrCreate(dbRoot.resolve("index/manifest.idx"));
        this.manifest.ensureFileExists();
        this.metaStore = PatternMetaStore.loadOrCreate(dbRoot.resolve("metadata/pattern-meta.json"));
        this.segmentWriters = new ConcurrentHashMap<>();
        this.tracer = new NoOpTracer();

        for (String segmentName : manifest.getAllSegmentNames()) {
            Path segPath = dbRoot.resolve("segments/" + segmentName);
            segmentWriters.put(segmentName, new SegmentWriter(segPath));
        }

        this.shardSelectorRef = new AtomicReference<>(createShardSelector());
    }

    private PhaseShardSelector createShardSelector() {
        var locations = manifest.getAllLocations();

        if (locations.isEmpty()) {
            return PhaseShardSelector.emptyFallback();
        }
        return PhaseShardSelector.fromManifest(locations, READ_EPSILON);
    }

    private void rebuildShardSelector() {
        this.shardSelectorRef.set(createShardSelector());
    }

    @Override
    public String insert(WavePattern psi, Map<String, String> metadata)
            throws DuplicatePatternException, InvalidWavePatternException {

        String idKey = HashingUtil.computeContentHash(psi);
        HashingUtil.parseAndValidateMd5(idKey);

        globalLock.writeLock().lock();
        try {
            if (manifest.contains(idKey)) {
                throw new DuplicatePatternException(idKey);
            }

            double phaseCenter = Arrays.stream(psi.phase()).average().orElse(0.0);
            PhaseShardSelector selector = shardSelectorRef.get();
            String segmentName = selector.selectShard(psi);
            String finalSegmentName = segmentName;
            double existingCenter = manifest.getAllLocations().stream()
                    .filter(loc -> loc.segmentName().equals(finalSegmentName))
                    .mapToDouble(ManifestIndex.PatternLocation::phaseCenter)
                    .average()
                    .orElse(phaseCenter);

            double EPS = 0.1;

            if (Math.abs(phaseCenter - existingCenter) > EPS) {
                segmentName = "phase-" + manifest.getAllSegmentNames().size() + ".segment";
            }

            SegmentWriter writer = getOrCreateWriter(segmentName);

            long offset = writer.write(idKey, psi);
            writer.flush();
            manifest.add(idKey, segmentName, offset, phaseCenter);
            if (!metadata.isEmpty()) {
                metaStore.put(idKey, metadata);
                metaStore.flush();
            }

            rebuildShardSelector();
            return idKey;

        } catch (SegmentOverflowException | DuplicatePatternException | InvalidWavePatternException e) {
            throw e;
        } catch (Exception e) {
            manifest.remove(idKey);
            metaStore.remove(idKey);
            throw new RuntimeException("Insert failed: " + idKey, e);
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String idKey) throws PatternNotFoundException {
        globalLock.writeLock().lock();
        try {
            HashingUtil.parseAndValidateMd5(idKey);
            ManifestIndex.PatternLocation loc = manifest.get(idKey);
            if (loc == null) throw new PatternNotFoundException(idKey);

            SegmentWriter writer = getOrCreateWriter(loc.segmentName());
            try {
                writer.markDeleted(loc.offset());
            } catch (PatternNotFoundException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to mark deleted: " + idKey, e);
            }

            if (manifest.contains(idKey)) manifest.remove(idKey);
            if (metaStore.contains(idKey)) metaStore.remove(idKey);

            rebuildShardSelector();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    @Override
    public String replace(String oldId, WavePattern newPattern, Map<String, String> newMetadata)
            throws PatternNotFoundException, InvalidWavePatternException {

        if (newPattern.amplitude().length != newPattern.phase().length) {
            throw new InvalidWavePatternException("Amplitude / phase length mismatch");
        }

        globalLock.writeLock().lock();
        try {
            HashingUtil.parseAndValidateMd5(oldId);
            ManifestIndex.PatternLocation oldLoc = manifest.get(oldId);
            if (oldLoc == null) throw new PatternNotFoundException(oldId);

            String newId = HashingUtil.computeContentHash(newPattern);
            HashingUtil.parseAndValidateMd5(newId);
            if (manifest.contains(newId)) {
                throw new DuplicatePatternException("Replacement target would collide with existing pattern: " + newId);
            }

            double phaseCenter = Arrays.stream(newPattern.phase()).average().orElse(0.0);
            String newSegment = shardSelectorRef.get().selectShard(newPattern);
            SegmentWriter newWriter = getOrCreateWriter(newSegment);
            long newOffset = newWriter.write(newId, newPattern);
            newWriter.flush();
            manifest.add(newId, newSegment, newOffset, phaseCenter);
            if (!newMetadata.isEmpty()) {
                metaStore.put(newId, newMetadata);
                metaStore.flush();
            }

            SegmentWriter oldWriter = getOrCreateWriter(oldLoc.segmentName());
            oldWriter.markDeleted(oldLoc.offset());
            manifest.remove(oldId);
            metaStore.remove(oldId);

            manifest.flush();
            rebuildShardSelector();

            return newId;

        } catch (SegmentOverflowException | DuplicatePatternException |
                 InvalidWavePatternException | PatternNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Replace failed: " + oldId, e);
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    @Override
    public List<ResonanceMatch> query(WavePattern query, int topK) {
        globalLock.readLock().lock();
        try {
            String queryId = HashingUtil.computeContentHash(query);

            PriorityQueue<HeapItem> heap = new PriorityQueue<>(
                    topK, Comparator.comparingDouble(HeapItem::priority));

            for (String shard : segmentWriters.keySet()) {
                collectMatchesFromShard(shard, query, queryId, topK, heap);
            }

            Comparator<HeapItem> order = Comparator
                    .comparingDouble(HeapItem::priority).reversed()
                    .thenComparing((HeapItem h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            return heap.stream()
                    .sorted(order)
                    .map(HeapItem::match)
                    .toList();

        } finally {
            globalLock.readLock().unlock();
        }
    }

    @Override
    public List<ResonanceMatchDetailed> queryDetailed(WavePattern query, int topK) {
        globalLock.readLock().lock();
        try {
            String queryId = HashingUtil.computeContentHash(query);

            PriorityQueue<HeapItemDetailed> heap = new PriorityQueue<>(
                    topK, Comparator.comparingDouble(HeapItemDetailed::priority));

            for (String shard : segmentWriters.keySet()) {
                collectDetailedFromShard(shard, query, queryId, topK, heap);
            }

            Comparator<HeapItemDetailed> order = Comparator
                    .comparingDouble(HeapItemDetailed::priority).reversed()
                    .thenComparing((HeapItemDetailed h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            return heap.stream()
                    .sorted(order)
                    .map(HeapItemDetailed::match)
                    .toList();

        } finally {
            globalLock.readLock().unlock();
        }
    }

    @Override
    public InterferenceMap queryInterference(WavePattern query, int topK) {
        return new InterferenceMap(query, queryDetailed(query, topK));
    }

    @Override
    public List<InterferenceEntry> queryInterferenceMap(WavePattern query, int topK) {
        return queryDetailed(query, topK).stream()
                .map(m -> new InterferenceEntry(
                        m.id(),
                        m.energy(),
                        m.phaseDelta(),
                        m.zone(),
                        m.pattern()
                ))
                .toList();
    }

    @Override
    public List<ResonanceMatch> queryComposite(List<WavePattern> patterns, List<Double> weights, int topK) {
        Objects.requireNonNull(patterns, "patterns must not be null");
        if (patterns.isEmpty()) throw new InvalidWavePatternException("Empty pattern list in composite query");

        WavePattern superposed = WavePatternUtils.superpose(patterns, weights);
        return query(superposed, topK);
    }

    @Override
    public List<ResonanceMatchDetailed> queryCompositeDetailed(List<WavePattern> patterns, List<Double> weights, int topK) {
        Objects.requireNonNull(patterns, "patterns must not be null");
        if (patterns.isEmpty()) throw new InvalidWavePatternException("Empty pattern list in composite query");

        WavePattern superposed = WavePatternUtils.superpose(patterns, weights);
        return queryDetailed(superposed, topK);
    }

    @Override
    public float compare(WavePattern a, WavePattern b) {
        return ResonanceEngine.compare(a, b);
    }

    private SegmentWriter getOrCreateWriter(String segmentName) {
        return segmentWriters.computeIfAbsent(segmentName, name -> {
            try {
                Path path = rootDir.resolve("segments/" + name);
                Files.createDirectories(path.getParent());
                if (Files.notExists(path)) {
                    Files.createFile(path);
                }
                return new SegmentWriter(path);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create segment writer: " + name, e);
            }
        });
    }

    @Override
    public void close() {
        globalLock.writeLock().lock();
        try {
            for (SegmentWriter writer : segmentWriters.values()) {
                writer.close();
            }
            manifest.flush();
            metaStore.flush();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    public PhaseShardSelector getShardSelector() {
        return shardSelectorRef.get();
    }

    private void collectDetailedFromShard(String shard,
                                          WavePattern query,
                                          String queryId,
                                          int topK,
                                          PriorityQueue<HeapItemDetailed> heap) {

        SegmentWriter writer = segmentWriters.get(shard);
        if (writer == null) return;

        try (SegmentReader reader = new SegmentReader(writer.getPath())) {

            Map<String, SegmentReader.PatternWithId> latestValidEntries = new HashMap<>();

            for (SegmentReader.PatternWithId entry : reader.readAllWithId()) {
                ManifestIndex.PatternLocation loc = manifest.get(entry.id());
                if (loc == null) continue;
                if (!loc.segmentName().equals(shard)) continue;
                if (loc.offset() != entry.offset()) continue;
                latestValidEntries.put(entry.id(), entry);
            }

            for (SegmentReader.PatternWithId entry : latestValidEntries.values()) {
                WavePattern cand = entry.pattern();
                if (cand.amplitude().length != query.amplitude().length) continue;

                ComparisonResult result = ResonanceEngine.compareWithPhaseDelta(query, cand);
                float energy = result.energy();
                double phaseShift = result.phaseDelta();
                ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
                double zoneScore = ResonanceZoneClassifier.computeScore(energy, phaseShift);

                ResonanceMatchDetailed match = new ResonanceMatchDetailed(
                        entry.id(), energy, cand, phaseShift, zone, zoneScore
                );

                boolean idEq = entry.id().equals(queryId);
                boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
                double priority = energy + (idEq ? 1.0 : 0.0) + (exactEq ? 0.5 : 0.0);

                heap.add(new HeapItemDetailed(match, priority));
                if (heap.size() > topK) heap.poll();
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to read segment: " + shard, e);
        }
    }

    private void collectMatchesFromShard(String shard,
                                         WavePattern query,
                                         String queryId,
                                         int topK,
                                         PriorityQueue<HeapItem> heap) {

        SegmentWriter writer = segmentWriters.get(shard);
        if (writer == null) return;

        try (SegmentReader reader = new SegmentReader(writer.getPath())) {

            Map<String, SegmentReader.PatternWithId> latestValidEntries = new HashMap<>();

            for (SegmentReader.PatternWithId entry : reader.readAllWithId()) {
                ManifestIndex.PatternLocation loc = manifest.get(entry.id());
                if (loc == null) continue;
                if (!loc.segmentName().equals(shard)) continue;
                if (loc.offset() != entry.offset()) continue;

                latestValidEntries.put(entry.id(), entry);
            }

            for (SegmentReader.PatternWithId entry : latestValidEntries.values()) {
                WavePattern cand = entry.pattern();
                if (cand.amplitude().length != query.amplitude().length) continue;

                float base = ResonanceEngine.compare(query, cand);
                boolean idEq = entry.id().equals(queryId);
                boolean exactEq = base > 1.0f - EXACT_MATCH_EPS;
                float priority = base + (idEq ? 1.0f : 0.0f) + (exactEq ? 0.5f : 0.0f);

                tracer.trace(entry.id(), query, cand, base);

                HeapItem item = new HeapItem(new ResonanceMatch(entry.id(), base, cand), priority);
                if (heap.size() < topK) {
                    heap.add(item);
                } else {
                    HeapItem head = heap.peek();
                    if (head != null && priority > head.priority()) {
                        heap.poll();
                        heap.add(item);
                    }
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to read segment: " + shard, e);
        }
    }
}