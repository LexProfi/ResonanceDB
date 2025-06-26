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
import ai.evacortex.resonancedb.core.storage.io.CachedReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentCache;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import ai.evacortex.resonancedb.core.storage.responce.*;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
import ai.evacortex.resonancedb.core.storage.util.NoOpTracer;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class WavePatternStoreImpl implements ResonanceStore, Closeable {

    private static final double BUCKET_WIDTH_RAD = Double.parseDouble(System.getProperty("segment.bucketRad", "0.2"));
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
    private final SegmentCache readerCache;

    public WavePatternStoreImpl(Path dbRoot) {
        this.rootDir = dbRoot;
        this.manifest = ManifestIndex.loadOrCreate(dbRoot.resolve("index/manifest.idx"));
        this.manifest.ensureFileExists();
        this.metaStore = PatternMetaStore.loadOrCreate(dbRoot.resolve("metadata/pattern-meta.json"));
        this.segmentGroups = new ConcurrentHashMap<>();
        this.tracer = new NoOpTracer();
        this.compactor = new DefaultSegmentCompactor(manifest, metaStore, rootDir.resolve("segments"), globalLock);
        this.readerCache = new SegmentCache(rootDir.resolve("segments"));

        loadAllWritersFromManifest();

        this.shardSelectorRef = new AtomicReference<>(createShardSelector());

        scheduler.scheduleAtFixedRate(() -> {
            for (String phase : segmentGroups.keySet()) {
                compactPhase(phase);
            }
        }, 10, 5, TimeUnit.MINUTES);
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

        if (psi.amplitude().length != psi.phase().length) {
            throw new InvalidWavePatternException("Amplitude / phase length mismatch");
        }

        String idKey = HashingUtil.computeContentHash(psi);
        HashingUtil.parseAndValidateMd5(idKey);
        if (manifest.contains(idKey)) {
            throw new DuplicatePatternException(idKey);
        }
        globalLock.writeLock().lock();
        try {
            double phaseCenter = Arrays.stream(psi.phase()).average().orElse(0.0);
            int bucket   = (int) Math.floor((phaseCenter + Math.PI) / BUCKET_WIDTH_RAD);
            String base  = "phase-" + bucket;
            PhaseSegmentGroup group = getOrCreateGroup(base);
            double existingCenter = group.getAvgPhase();
            SegmentWriter writer = group.getWritable();
            boolean sizeOverflow = writer == null || writer.willOverflow(psi);
            boolean phaseOverflow = Math.abs(phaseCenter - existingCenter) > 0.15;
            if (sizeOverflow || phaseOverflow) {
                writer = group.createAndRegisterNewSegment();
            }
            group.registerIfAbsent(writer);
            long offset;
            while (true) {
                try {
                    offset = writer.write(idKey, psi);
                    writer.flush();
                    writer.ensureExists();
                    writer.sync();
                    //System.out.println("[INSERT] Updated readerCache for segment: " + writer.getSegmentName() +" @ offset=" + offset);
                    registerSegment(writer);
                    break;
                } catch (SegmentOverflowException _e) {
                    writer = group.createAndRegisterNewSegment();
                    group.registerIfAbsent(writer);
                }
            }
            manifest.add(idKey, writer.getSegmentName(), offset, phaseCenter);
            manifest.flush();
            if (!metadata.isEmpty()) {
                metaStore.put(idKey, metadata);
                metaStore.flush();
            }
            group.updatePhaseStats(phaseCenter);

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
            writer.markDeleted(loc.offset());
            long newVer = writer.flush();
            writer.sync();
            readerCache.updateVersion(writer.getSegmentName(), newVer);
            manifest.remove(idKey);
            metaStore.remove(idKey);
            manifest.flush();
            metaStore.flush();
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

        HashingUtil.parseAndValidateMd5(oldId);
        String newId = HashingUtil.computeContentHash(newPattern);
        HashingUtil.parseAndValidateMd5(newId);

        globalLock.writeLock().lock();
        try {
            ManifestIndex.PatternLocation oldLoc = manifest.get(oldId);
            if (oldLoc == null) throw new PatternNotFoundException(oldId);

            if (!oldId.equals(newId) && manifest.contains(newId)) {
                throw new DuplicatePatternException("Replacement would collide: " + newId);
            }

            double phaseCenter = Arrays.stream(newPattern.phase()).average().orElse(0.0);
            int bucket = (int) Math.floor((phaseCenter + Math.PI) / BUCKET_WIDTH_RAD);
            String base = "phase-" + bucket;
            PhaseSegmentGroup group = getOrCreateGroup(base);
            SegmentWriter writer = group.getWritable();
            if (writer == null || writer.willOverflow(newPattern)) {
                writer = group.createAndRegisterNewSegment();
            }
            group.registerIfAbsent(writer);

            long offset, newVer;
            while (true) {
                try {
                    offset = writer.write(newId, newPattern);
                    newVer = writer.flush();
                    writer.sync();
                    break;
                } catch (SegmentOverflowException _e) {
                    writer = group.createAndRegisterNewSegment();
                    group.registerIfAbsent(writer);
                }
            }

            SegmentWriter oldWriter = null;
            if (!oldLoc.segmentName().equals(writer.getSegmentName()) || oldLoc.offset() != offset) {
                oldWriter = getOrCreateWriter(oldLoc.segmentName());
                oldWriter.markDeleted(oldLoc.offset());
                oldWriter.flush();
                oldWriter.sync();
            }

            if (oldId.equals(newId)) {
                manifest.replace(oldId, oldLoc.segmentName(), oldLoc.offset(), writer.getSegmentName(), offset, phaseCenter);
            } else {
                manifest.remove(oldId);
                manifest.add(newId, writer.getSegmentName(), offset, phaseCenter);
            }
            group.updatePhaseStats(phaseCenter);
            if (!newMetadata.isEmpty()) {
                metaStore.put(newId, newMetadata);
            }

            manifest.flush();
            metaStore.flush();
            readerCache.updateVersion(writer.getSegmentName(), writer.getWriteOffset());

            if(oldWriter != null) {
                readerCache.updateVersion(oldWriter.getSegmentName(), oldWriter.getWriteOffset());
            }
            rebuildShardSelector();
            //System.out.println("[REPLACE] oldId=" + oldId + ", newId=" + newId);
            //System.out.println("[REPLACE] oldLoc=" + oldLoc.segmentName() + " @ " + oldLoc.offset());
            //System.out.println("[REPLACE] wrote to " + writer.getSegmentName() + " @ offset=" + offset + " ver=" + newVer);
            return newId;

        } finally {
            globalLock.writeLock().unlock();
        }
    }

    @Override
    public List<ResonanceMatch> query(WavePattern query, int topK) {
        globalLock.readLock().lock();
        try {
            //System.out.println("[QUERY] ⏳ Starting query(topK=" + topK + ") → segmentGroups: " + segmentGroups.size());
            String queryId = HashingUtil.computeContentHash(query);
            Comparator<HeapItem> order = Comparator
                    .comparingDouble(HeapItem::priority).reversed()
                    .thenComparing((HeapItem h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<CompletableFuture<List<HeapItem>>> futures = segmentGroups.values().stream()
                    .flatMap(group -> {
                        Stream<SegmentWriter> base = group.getAll().stream();
                        SegmentWriter w = group.getWritable();
                        List<SegmentWriter> all = (w == null)
                                ? base.toList()
                                : Stream.concat(base, Stream.of(w)).toList();
                        //all.forEach(sw -> //System.out.println("[QUERY] ↪ SegmentWriter: " + sw.getSegmentName()));
                        return all.stream();
                    })
                    .distinct()
                    .map(writer -> CompletableFuture.supplyAsync(() ->
                            collectMatchesFromWriter(writer, query, queryId, topK), executor))
                    .toList();

            List<HeapItem> collected = futures.stream()
                    .flatMap(f -> f.join().stream())
                    .toList();

            Map<String, HeapItem> best = new HashMap<>();
            for (HeapItem hi : collected) {
                best.merge(hi.match().id(), hi,
                        (a, b) -> a.priority() >= b.priority() ? a : b);
            }

            return best.values().stream()
                    .sorted(order)
                    .limit(topK)
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

            Comparator<HeapItemDetailed> order = Comparator
                    .comparingDouble(HeapItemDetailed::priority).reversed()
                    .thenComparing((HeapItemDetailed h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<CompletableFuture<List<HeapItemDetailed>>> futures = segmentGroups.values().stream()
                    .flatMap(group -> {
                        Stream<SegmentWriter> base = group.getAll().stream();
                        SegmentWriter w = group.getWritable();
                        return (w == null) ? base : Stream.concat(base, Stream.of(w));
                    })
                    .distinct()
                    .map(writer -> CompletableFuture.supplyAsync(() ->
                            collectDetailedFromWriter(writer, query, queryId, topK), executor))
                    .toList();

            List<HeapItemDetailed> collected = futures.stream()
                    .flatMap(f -> f.join().stream())
                    .toList();

            Map<String, HeapItemDetailed> best = new HashMap<>();
            for (HeapItemDetailed hid : collected) {
                best.merge(hid.match().id(), hid,
                        (a, b) -> a.priority() >= b.priority() ? a : b);
            }

            return best.values().stream()
                    .sorted(order)
                    .limit(topK)
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

        CachedReader reader = readerCache.get(writer.getSegmentName());
        if (reader == null) return List.of();

        PriorityQueue<HeapItem> localHeap =new PriorityQueue<>(topK, Comparator.comparingDouble(HeapItem::priority));
        //reader.lazyStream()
        //System.out.println("[QUERY] Reading from segment: " + writer.getSegmentName());
        Set<String> ids = new HashSet<>(reader.allIds());
        //System.out.println("[DEBUG] CachedReader IDs = " + ids.size());
        for (String id : ids) {
            if (!manifest.contains(id)) {
                //System.out.println("⚠️  ID in reader but not in manifest: " + id);
            }
        }

        for (String id : reader.allIds()) {
            ManifestIndex.PatternLocation loc = manifest.get(id);
            if (loc == null || !loc.segmentName().equals(writer.getSegmentName())) continue;

            WavePattern cand;
            try {
                cand = reader.readById(id);
                //System.out.println("✅ Read candidate: " + id);
            } catch (PatternNotFoundException | InvalidWavePatternException e) {
                continue;
            }
            if (cand.amplitude().length != query.amplitude().length) {
                //System.out.println("⛔ Length mismatch: " + id + " → cand=" + cand.amplitude().length + ", query=" + query.amplitude().length);
                continue;
            }
            float base = ResonanceEngine.compare(query, cand);
            boolean idEq = id.equals(queryId);
            boolean exactEq = base > 1.0f - EXACT_MATCH_EPS;
            float priority = base + (idEq ? 1.0f : 0.0f) + (exactEq ? 0.5f : 0.0f);

            tracer.trace(id, query, cand, base);

            HeapItem item = new HeapItem(new ResonanceMatch(id, base, cand), priority);
            if (localHeap.size() < topK) {
                if (idEq) {
                    //System.out.println("🔥 Match is exact by ID (" + id + "), energy=" + base + ", exact=" + exactEq);
                }
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
    }

    private List<HeapItemDetailed> collectDetailedFromWriter(SegmentWriter writer,
                                                             WavePattern query,
                                                             String queryId,
                                                             int topK) {
        if (writer == null) return List.of();

        CachedReader reader = readerCache.get(writer.getSegmentName());
        if (reader == null) return List.of();

        PriorityQueue<HeapItemDetailed> localHeap = new PriorityQueue<>(topK, Comparator.comparingDouble(HeapItemDetailed::priority));
        //reader.lazyStream()
        for (String id : reader.allIds()) {
            ManifestIndex.PatternLocation loc = manifest.get(id);
            if (loc == null || !loc.segmentName().equals(writer.getSegmentName())) continue;

            WavePattern cand;
            try {
                cand = reader.readById(id);
            } catch (PatternNotFoundException | InvalidWavePatternException e) {
                continue;
            }

            if (cand.amplitude().length != query.amplitude().length) continue;

            ComparisonResult result = ResonanceEngine.compareWithPhaseDelta(query, cand);
            float energy = result.energy();
            double phaseShift = result.phaseDelta();

            ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
            double zoneScore = switch (zone) {
                case CORE   -> 2.0;
                case FRINGE -> 1.0;
                case SHADOW -> 0.0;
            };

            ResonanceMatchDetailed match = new ResonanceMatchDetailed(
                    id, energy, cand, phaseShift, zone, zoneScore
            );

            boolean idEq = id.equals(queryId);
            boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
            double priority = zoneScore + energy + (idEq   ? 1.0 : 0.0) + (exactEq? 0.5 : 0.0);

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
            readerCache.close();

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

        } finally {
            globalLock.writeLock().unlock();
        }
    }

    public boolean containsExactPattern(WavePattern pattern) {
        String id = HashingUtil.computeContentHash(pattern);
        return manifest.contains(id);
    }

    private void registerSegment(SegmentWriter w) {
        manifest.registerSegmentIfAbsent(w.getSegmentName());
        segmentGroups.computeIfAbsent(base(w.getSegmentName()),   // без пробела
                        k -> new PhaseSegmentGroup(k, rootDir.resolve("segments"), compactor))
                .registerIfAbsent(w);

        readerCache.updateVersion(w.getSegmentName(), w.getWriteOffset());
    }

    private void loadAllWritersFromManifest() {
        for (String seg : manifest.getAllSegmentNames()) {
            PhaseSegmentGroup g = segmentGroups.computeIfAbsent(
                    base(seg),
                    k -> new PhaseSegmentGroup(k, rootDir.resolve("segments"), compactor));

            SegmentWriter w = g.containsSegment(seg)      // если уже создан
                    ? g.getAll().stream()
                    .filter(sw -> sw.getSegmentName().equals(seg))
                    .findFirst().orElseThrow()
                    : new SegmentWriter(rootDir.resolve("segments").resolve(seg));
            registerSegment(w);
        }
    }
    private static String base(String segmentName) {
        int dash = segmentName.indexOf('-');
        if (dash > 0) {
            return segmentName.substring(0, dash);
        }
        if (segmentName.endsWith(".segment")) {
            return segmentName.substring(0, segmentName.length() - ".segment".length());
        }
        return segmentName;
    }
}