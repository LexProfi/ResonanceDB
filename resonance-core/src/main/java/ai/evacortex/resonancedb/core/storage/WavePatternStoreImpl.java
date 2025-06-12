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
import ai.evacortex.resonancedb.core.storage.compactor.DefaultSegmentCompactor;
import ai.evacortex.resonancedb.core.storage.compactor.SegmentCompactor;
import ai.evacortex.resonancedb.core.storage.io.SegmentReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import ai.evacortex.resonancedb.core.storage.responce.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WavePatternStoreImpl implements ResonanceStore, Closeable {

    private static final double READ_EPSILON  = 0.1;
    private static final float EXACT_MATCH_EPS = 1e-6f;
    private final ManifestIndex manifest;
    private final PatternMetaStore metaStore;
    private final Map<String, PhaseSegmentGroup> segmentGroups;
    private final Path rootDir;
    private final AtomicReference<PhaseShardSelector> shardSelectorRef;
    private final ResonanceTracer tracer;
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();
    private record HeapItem(ResonanceMatch match, float priority) {}
    private record HeapItemDetailed(ResonanceMatchDetailed match, double priority) {}
    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final SegmentCompactor compactor;

    public WavePatternStoreImpl(Path dbRoot) {
        this.rootDir = dbRoot;
        this.manifest = ManifestIndex.loadOrCreate(dbRoot.resolve("index/manifest.idx"));
        this.manifest.ensureFileExists();
        this.metaStore = PatternMetaStore.loadOrCreate(dbRoot.resolve("metadata/pattern-meta.json"));
        this.segmentGroups = new ConcurrentHashMap<>();
        this.tracer = new NoOpTracer();
        this.compactor = new DefaultSegmentCompactor(manifest, metaStore, rootDir.resolve("segments"), globalLock);

        for (String segmentName : manifest.getAllSegmentNames()) {
            String base = segmentName.split("-")[0];
            segmentGroups.computeIfAbsent(base,
                    key -> new PhaseSegmentGroup(key, rootDir.resolve("segments"), compactor));
        }

        this.shardSelectorRef = new AtomicReference<>(createShardSelector());

        scheduler.scheduleAtFixedRate(() -> {
            for (String phase : segmentGroups.keySet()) {
                compactPhase(phase);
            }
        }, 10, 300, TimeUnit.SECONDS);
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
            String selectedShard = shardSelectorRef.get().selectShard(psi);
            String baseName = selectedShard.split("-")[0];

            PhaseSegmentGroup group = getOrCreateGroup(baseName);

            double existingCenter = manifest.getAllLocations().stream()
                    .filter(l -> l.segmentName().startsWith(baseName))
                    .mapToDouble(ManifestIndex.PatternLocation::phaseCenter)
                    .average()
                    .orElse(phaseCenter);

            SegmentWriter writer = (Math.abs(phaseCenter - existingCenter) > 0.1)
                    ? group.createAndRegisterNewSegment()
                    : group.getWritable();

            String segmentName = writer.getSegmentName();
            long offset = writer.write(idKey, psi);
            writer.flush();
            writer.sync();
            manifest.add(idKey, segmentName, offset, phaseCenter);
            if (!metadata.isEmpty()) {
                metaStore.put(idKey, metadata);
            }
            manifest.flush();
            metaStore.flush();
            rebuildShardSelector();
            return idKey;

        } catch (SegmentOverflowException |
                 DuplicatePatternException |
                 InvalidWavePatternException e) {
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

            metaStore.flush();
            rebuildShardSelector();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    @Override
    public String replace(String oldId,
                          WavePattern newPattern,
                          Map<String, String> newMetadata)
            throws PatternNotFoundException, InvalidWavePatternException {

        if (newPattern.amplitude().length != newPattern.phase().length) {
            throw new InvalidWavePatternException("Amplitude / phase length mismatch");
        }

        HashingUtil.parseAndValidateMd5(oldId);
        ManifestIndex.PatternLocation oldLoc;
        globalLock.readLock().lock();
        try {
            oldLoc = manifest.get(oldId);
            if (oldLoc == null) throw new PatternNotFoundException(oldId);
        } finally {
            globalLock.readLock().unlock();
        }
        String newId = HashingUtil.computeContentHash(newPattern);
        HashingUtil.parseAndValidateMd5(newId);
        if (oldId.equals(newId)) {
            SegmentWriter writer = getOrCreateWriter(oldLoc.segmentName());
            long offset = writer.write(newId, newPattern);
            writer.flush();
            writer.sync();

            globalLock.writeLock().lock();
            try {
                manifest.add(newId, writer.getSegmentName(), offset, oldLoc.phaseCenter());
                if (!newMetadata.isEmpty()) metaStore.put(newId, newMetadata);
                manifest.flush();
                metaStore.flush();
                rebuildShardSelector();
            } finally {
                globalLock.writeLock().unlock();
            }
            return newId;
        }

        globalLock.readLock().lock();
        try {
            if (manifest.contains(newId)) {
                throw new DuplicatePatternException("Replacement would collide: " + newId);
            }
        } finally {
            globalLock.readLock().unlock();
        }

        double phaseCenter = Arrays.stream(newPattern.phase()).average().orElse(0.0);
        String shard = shardSelectorRef.get().selectShard(newPattern);
        String baseName = shard.split("-")[0];
        PhaseSegmentGroup group = getOrCreateGroup(baseName);
        SegmentWriter writer = group.getWritable();

        long offset = writer.write(newId, newPattern);
        writer.flush();
        writer.sync();

        globalLock.writeLock().lock();
        try {
            group.registerIfAbsent(writer);

            manifest.add(newId, writer.getSegmentName(), offset, phaseCenter);
            if (!newMetadata.isEmpty()) metaStore.put(newId, newMetadata);

            SegmentWriter oldWriter = getOrCreateWriter(oldLoc.segmentName());
            oldWriter.markDeleted(oldLoc.offset());
            manifest.remove(oldId);
            metaStore.remove(oldId);

            manifest.flush();
            metaStore.flush();
            rebuildShardSelector();
        } finally {
            globalLock.writeLock().unlock();
        }

        return newId;
    }


    @Override
    public List<ResonanceMatch> query(WavePattern query, int topK) {
        globalLock.readLock().lock();
        try {
            String queryId = HashingUtil.computeContentHash(query);
            Comparator<HeapItem> order = Comparator
                    .comparingDouble(HeapItem::priority).reversed()
                    .thenComparing((HeapItem h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<CompletableFuture<List<HeapItem>>> futures = segmentGroups.values().stream()
                    .flatMap(group -> group.getAll().stream())
                    .map(writer -> CompletableFuture.supplyAsync(() ->
                            collectMatchesFromWriter(writer, query, queryId, topK), executor))
                    .toList();

            List<HeapItem> all = futures.stream()
                    .peek(_ -> Thread.yield())
                    .flatMap(f -> f.join().stream())
                    .sorted(order)
                    .limit(topK)
                    .toList();
            return all.stream().map(HeapItem::match).toList();

        } finally {
            globalLock.readLock().unlock();
        }
    }

    @Override
    public List<ResonanceMatchDetailed> queryDetailed(WavePattern query, int topK) {
        globalLock.readLock().lock();
        try {
            String queryId = HashingUtil.computeContentHash(query);

            Comparator<HeapItemDetailed> order = Comparator
                    .comparingDouble(HeapItemDetailed::priority).reversed()
                    .thenComparing((HeapItemDetailed h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<CompletableFuture<List<HeapItemDetailed>>> futures = segmentGroups.values().stream()
                    .flatMap(group -> group.getAll().stream())
                    .map(writer -> CompletableFuture.supplyAsync(() ->
                            collectDetailedFromWriter(writer, query, queryId, topK), executor))
                    .toList();

            List<HeapItemDetailed> all = futures.stream()
                    .flatMap(future -> future.join().stream())
                    .sorted(order)
                    .limit(topK)
                    .toList();

            return all.stream().map(HeapItemDetailed::match).toList();

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
        String groupKey = segmentName.contains("-") ?
                segmentName.split("-")[0] : segmentName.replace(".segment", "");

        PhaseSegmentGroup group = segmentGroups.computeIfAbsent(groupKey, key ->
                new PhaseSegmentGroup(
                        key,
                        rootDir.resolve("segments"),
                        this.compactor
                )
        );

        return group.getWritable();
    }

    public PhaseShardSelector getShardSelector() {
        return shardSelectorRef.get();
    }

    private List<HeapItem> collectMatchesFromWriter(SegmentWriter writer,
                                                    WavePattern query,
                                                    String queryId,
                                                    int topK) {

        if (writer == null) return List.of();

        try (SegmentReader reader = new SegmentReader(writer.getPath())) {

            Map<String, SegmentReader.PatternWithId> latestValidEntries = new HashMap<>();

            for (SegmentReader.PatternWithId entry : reader.readAllWithId()) {
                ManifestIndex.PatternLocation loc = manifest.get(entry.id());
                if (loc == null) continue;
                if (!loc.segmentName().equals(writer.getSegmentName())) continue;
                if (loc.offset() != entry.offset()) continue;
                latestValidEntries.put(entry.id(), entry);
            }

            PriorityQueue<HeapItem> localHeap = new PriorityQueue<>(topK, Comparator.comparingDouble(HeapItem::priority));

            for (SegmentReader.PatternWithId entry : latestValidEntries.values()) {
                WavePattern cand = entry.pattern();
                if (cand.amplitude().length != query.amplitude().length) continue;

                float base = ResonanceEngine.compare(query, cand);
                boolean idEq = entry.id().equals(queryId);
                boolean exactEq = base > 1.0f - EXACT_MATCH_EPS;
                float priority = base + (idEq ? 1.0f : 0.0f) + (exactEq ? 0.5f : 0.0f);

                tracer.trace(entry.id(), query, cand, base);

                HeapItem item = new HeapItem(new ResonanceMatch(entry.id(), base, cand), priority);
                if (localHeap.size() < topK) {
                    localHeap.add(item);
                } else {
                    HeapItem head = localHeap.peek();
                    if (head == null || priority > head.priority()) {
                        localHeap.poll();
                        localHeap.add(item);
                    }
                }
            }
            return new ArrayList<>(localHeap);

        } catch (IOException e) {
            throw new RuntimeException("Failed to read segment: " + writer.getSegmentName(), e);
        }
    }

    private List<HeapItemDetailed> collectDetailedFromWriter(SegmentWriter writer,
                                                             WavePattern query,
                                                             String queryId,
                                                             int topK) {
        if (writer == null) return List.of();
        try (SegmentReader reader = new SegmentReader(writer.getPath())) {

            Map<String, SegmentReader.PatternWithId> latestValidEntries = new HashMap<>();

            for (SegmentReader.PatternWithId entry : reader.readAllWithId()) {
                ManifestIndex.PatternLocation loc = manifest.get(entry.id());
                if (loc == null) continue;
                if (!loc.segmentName().equals(writer.getSegmentName())) continue;
                if (loc.offset() != entry.offset()) continue;
                latestValidEntries.put(entry.id(), entry);
            }

            PriorityQueue<HeapItemDetailed> localHeap = new PriorityQueue<>(topK, Comparator.comparingDouble(HeapItemDetailed::priority));

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

                HeapItemDetailed item = new HeapItemDetailed(match, priority);
                if (localHeap.size() < topK) {
                    localHeap.add(item);
                } else {
                    HeapItemDetailed head = localHeap.peek();
                    if (head == null || priority > head.priority()) {
                        localHeap.poll();
                        localHeap.add(item);
                    }
                }
            }
            return new ArrayList<>(localHeap);

        } catch (IOException e) {
            throw new RuntimeException("Failed to read segment: " + writer.getSegmentName(), e);
        }
    }

    private PhaseSegmentGroup getOrCreateGroup(String baseName) {
        return segmentGroups.computeIfAbsent(baseName,
                name -> new PhaseSegmentGroup(
                        name,
                        rootDir.resolve("segments"),
                        this.compactor
                ));
    }

    public void compactPhase(String baseName) {
        PhaseSegmentGroup group = segmentGroups.get(baseName);
        if (group != null && group.maybeCompact()) {
            rebuildShardSelector();
        }
    }

    @Override
    public void close() {
        globalLock.writeLock().lock();
        try {
            for (PhaseSegmentGroup group : segmentGroups.values()) {
                for (SegmentWriter writer : group.getAll()) {
                    try {
                        writer.close();
                    } catch (Exception e) {
                        System.err.println("Failed to close writer: " + writer.getSegmentName());
                    }
                }
            }

            manifest.flush();
            metaStore.flush();

            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    System.err.println("Executor did not terminate in time, forcing shutdown...");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

        } finally {
            globalLock.writeLock().unlock();
        }
    }

    public boolean containsExactPattern(WavePattern pattern) {
        String id = HashingUtil.computeContentHash(pattern);
        return manifest.contains(id);
    }
}