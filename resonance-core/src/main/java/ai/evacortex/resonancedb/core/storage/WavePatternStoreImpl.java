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
import ai.evacortex.resonancedb.core.storage.util.AutoLock;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
import ai.evacortex.resonancedb.core.storage.util.NoOpTracer;

import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings("resource")
public class WavePatternStoreImpl implements ResonanceStore, Closeable {

    private static final double BUCKET_WIDTH_RAD = Double.parseDouble(System.getProperty("resonance.segment.bucketRad", "0.2"));
    private static final boolean FLUSH_ASYNC = Boolean.parseBoolean(System.getProperty("resonance.flush.async", "false"));
    private static final double READ_EPSILON = 0.1;
    private static final float EXACT_MATCH_EPS = 1e-6f;

    private final Path rootDir;
    private final ManifestIndex manifest;
    private final PatternMetaStore metaStore;
    private final Map<String, PhaseSegmentGroup> segmentGroups;

    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();
    private final AtomicReference<PhaseShardSelector> shardSelectorRef;

    private final SegmentCache readerCache;
    private final SegmentCompactor compactor;
    private final ResonanceTracer tracer;

    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private final FlushDispatcher flushDispatcher;

    private record HeapItem(ResonanceMatch match, float priority) {}
    private record HeapItemDetailed(ResonanceMatchDetailed match, double priority) {}
    private record SegmentWriteResult(SegmentWriter writer, long offset, long version) {}

    public WavePatternStoreImpl(Path dbRoot) {
        this.rootDir = dbRoot;
        this.manifest = ManifestIndex.loadOrCreate(dbRoot.resolve("index/manifest.idx"));
        this.manifest.ensureFileExists();
        this.metaStore = PatternMetaStore.loadOrCreate(dbRoot.resolve("metadata/pattern-meta.json"));
        this.readerCache = new SegmentCache(dbRoot.resolve("segments"));
        this.compactor = new DefaultSegmentCompactor(manifest, metaStore, rootDir.resolve("segments"), globalLock);
        this.segmentGroups = new ConcurrentHashMap<>();
        this.tracer = new NoOpTracer();
        this.executor = Executors.newWorkStealingPool();
        this.scheduler = Executors.newScheduledThreadPool(1);
        loadAllWritersFromManifest();
        this.shardSelectorRef = new AtomicReference<>(createShardSelector());
        scheduler.scheduleAtFixedRate(() ->
                segmentGroups.keySet().forEach(this::compactPhase), 10, 5, TimeUnit.MINUTES);
        this.flushDispatcher = FLUSH_ASYNC ? new FlushDispatcher(Duration.ofMillis(5)) : null;
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

        try (AutoLock ignored = AutoLock.write(globalLock)) {
            int bucket = computePhaseBucket(psi);
            String base = "phase-" + bucket;
            PhaseSegmentGroup group = getOrCreateGroup(base);
            double phaseCenter = Arrays.stream(psi.phase()).average().orElse(0.0);
            double existingCenter = group.getAvgPhase();

            boolean phaseOverflow = Math.abs(phaseCenter - existingCenter) > 0.15;
            SegmentWriter writer = group.getWritable();
            if (writer == null || writer.willOverflow(psi) || phaseOverflow) {
                writer = group.createAndRegisterNewSegment();
            }

            group.registerIfAbsent(writer);

            SegmentWriteResult result = writeToSegment(idKey, psi, group);
            manifest.add(idKey, result.writer().getSegmentName(), result.offset(), phaseCenter);
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
        }
    }

    @Override
    public void delete(String idKey) throws PatternNotFoundException {
        try (AutoLock ignored = AutoLock.write(globalLock)) {
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

        try (AutoLock ignored = AutoLock.write(globalLock)) {
            ManifestIndex.PatternLocation oldLoc = manifest.get(oldId);
            if (oldLoc == null) throw new PatternNotFoundException(oldId);

            if (!oldId.equals(newId) && manifest.contains(newId)) {
                throw new DuplicatePatternException("Replacement would collide: " + newId);
            }

            int bucket = computePhaseBucket(newPattern);
            String base = "phase-" + bucket;
            PhaseSegmentGroup group = getOrCreateGroup(base);
            double phaseCenter = Arrays.stream(newPattern.phase()).average().orElse(0.0);

            SegmentWriteResult result;
            try {
                result = writeToSegment(newId, newPattern, group);
            } catch (Exception e) {
                throw new RuntimeException("Failed to write new pattern during replace", e);
            }

            try {
                SegmentWriter oldWriter = null;
                if (!oldLoc.segmentName().equals(result.writer().getSegmentName())
                        || oldLoc.offset() != result.offset()) {
                    oldWriter = getOrCreateWriter(oldLoc.segmentName());
                    oldWriter.markDeleted(oldLoc.offset());
                    oldWriter.flush();
                    oldWriter.sync();
                }

                if (oldId.equals(newId)) {
                    manifest.replace(oldId, oldLoc.segmentName(), oldLoc.offset(),
                            result.writer().getSegmentName(), result.offset(), phaseCenter);
                } else {
                    manifest.replace(oldId, newId,
                            result.writer().getSegmentName(), result.offset(), phaseCenter);
                }

                if (!newMetadata.isEmpty()) {
                    metaStore.put(newId, newMetadata);
                }

                manifest.flush();
                metaStore.flush();
                group.updatePhaseStats(phaseCenter);
                readerCache.updateVersion(result.writer().getSegmentName(), result.writer().getWriteOffset());

                if (oldWriter != null) {
                    readerCache.updateVersion(oldWriter.getSegmentName(), oldWriter.getWriteOffset());
                }

                rebuildShardSelector();
                return newId;

            } catch (Exception rollbackEx) {

                try {
                    SegmentWriter writer = result.writer();
                    writer.markDeleted(result.offset());
                    writer.flush();
                    writer.sync();
                    readerCache.updateVersion(writer.getSegmentName(), writer.getWriteOffset());
                } catch (Exception _) {
                    System.err.println("Failed to rollback written pattern " + newId);
                }
                throw new RuntimeException("Replace failed after write: " + newId, rollbackEx);
            }
        }
    }

    @Override
    public List<ResonanceMatch> query(WavePattern query, int topK) {
        try (AutoLock ignored = AutoLock.read(globalLock)) {
            String queryId = HashingUtil.computeContentHash(query);

            Comparator<HeapItem> order = Comparator
                    .comparingDouble(HeapItem::priority).reversed()
                    .thenComparing((HeapItem h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<SegmentWriter> writers = getAllWritersStream().toList();
            ForkJoinPool pool = (ForkJoinPool) executor;
            List<HeapItem> collected =
                    pool.invoke(new MatchQueryTask(writers, query, queryId, topK, 0, writers.size()));

            return deduplicateTopK(collected, h -> h.match().id(), order, topK)
                    .stream()
                    .map(HeapItem::match)
                    .toList();
        }
    }

    @Override
    public List<ResonanceMatchDetailed> queryDetailed(WavePattern query, int topK) {
        try (AutoLock ignored = AutoLock.read(globalLock)) {
            String queryId = HashingUtil.computeContentHash(query);

            Comparator<HeapItemDetailed> order = Comparator
                    .comparingDouble(HeapItemDetailed::priority).reversed()
                    .thenComparing((HeapItemDetailed h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<SegmentWriter> writers = getAllWritersStream().toList();
            ForkJoinPool pool = (ForkJoinPool) executor;
            List<HeapItemDetailed> collected =
                    pool.invoke(new DetailedMatchQueryTask(writers, query, queryId, topK, 0, writers.size()));

            return deduplicateTopK(collected, h -> h.match().id(), order, topK)
                    .stream()
                    .map(HeapItemDetailed::match)
                    .toList();
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
    public List<ResonanceMatchDetailed> queryCompositeDetailed(List<WavePattern> patterns, List<Double> weights,
                                                               int topK) {
        Objects.requireNonNull(patterns, "patterns must not be null");
        if (patterns.isEmpty()) throw new InvalidWavePatternException("Empty pattern list in composite query");

        WavePattern superposed = WavePatternUtils.superpose(patterns, weights);
        return queryDetailed(superposed, topK);
    }

    @Override
    public float compare(WavePattern a, WavePattern b) {
        return ResonanceEngine.compare(a, b);
    }

    private List<HeapItem> collectMatchesFromWriter(SegmentWriter writer, WavePattern query, String queryId, int topK) {

        if (writer == null) return List.of();

        return collectTopK(writer, topK, reader -> reader.allIds().stream()
                        .map(id -> {
                            ManifestIndex.PatternLocation loc = manifest.get(id);
                            if (loc == null || !loc.segmentName().equals(writer.getSegmentName())) return null;
                            WavePattern cand;
                            try {
                                cand = reader.readById(id);
                            } catch (PatternNotFoundException | InvalidWavePatternException e) {
                                return null;
                            }
                            if (cand.amplitude().length != query.amplitude().length) return null;

                            float energy = ResonanceEngine.compare(query, cand);
                            boolean idEq = id.equals(queryId);
                            boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
                            float priority = energy + (idEq ? 1.0f : 0.0f) + (exactEq ? 0.5f : 0.0f);

                            tracer.trace(id, query, cand, energy);
                            return new HeapItem(new ResonanceMatch(id, energy, cand), priority);
                        })
                        .filter(Objects::nonNull),
                Comparator.comparingDouble(HeapItem::priority)
        );
    }

    private List<HeapItemDetailed> collectDetailedFromWriter(SegmentWriter writer, WavePattern query, String queryId,
                                                             int topK) {

        if (writer == null) return List.of();

        return collectTopK(writer, topK, reader -> reader.allIds().stream()
                        .map(id -> {
                            ManifestIndex.PatternLocation loc = manifest.get(id);
                            if (loc == null || !loc.segmentName().equals(writer.getSegmentName())) return null;
                            WavePattern cand;
                            try {
                                cand = reader.readById(id);
                            } catch (PatternNotFoundException | InvalidWavePatternException e) {
                                return null;
                            }
                            if (cand.amplitude().length != query.amplitude().length) return null;

                            ComparisonResult result = ResonanceEngine.compareWithPhaseDelta(query, cand);
                            float energy = result.energy();
                            double phaseShift = result.phaseDelta();
                            ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
                            double zoneScore = switch (zone) {
                                case CORE   -> 2.0;
                                case FRINGE -> 1.0;
                                case SHADOW -> 0.0;
                            };

                            boolean idEq = id.equals(queryId);
                            boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
                            double priority = zoneScore + energy + (idEq ? 1.0 : 0.0) + (exactEq ? 0.5 : 0.0);

                            ResonanceMatchDetailed match = new ResonanceMatchDetailed(
                                    id, energy, cand, phaseShift, zone, zoneScore
                            );
                            return new HeapItemDetailed(match, priority);
                        })
                        .filter(Objects::nonNull),
                Comparator.comparingDouble(HeapItemDetailed::priority)
        );
    }

    private SegmentWriter getOrCreateWriter(String segmentName) {
        String groupKey = segmentName.contains("-") ?
                segmentName.split("-")[0] : segmentName.replace(".segment", "");

        PhaseSegmentGroup group = segmentGroups.computeIfAbsent(groupKey,
                key -> new PhaseSegmentGroup(key, rootDir.resolve("segments"), this.compactor)
        );

        return group.getWritable();
    }

    private PhaseShardSelector createShardSelector() {
        var locations = manifest.getAllLocations();
        return locations.isEmpty()
                ? PhaseShardSelector.emptyFallback()
                : PhaseShardSelector.fromManifest(locations, READ_EPSILON);
    }

    private void rebuildShardSelector() {
        shardSelectorRef.set(createShardSelector());
    }

    private String base(String segmentName) {
        int dash = segmentName.indexOf('-');
        if (dash > 0) return segmentName.substring(0, dash);
        return segmentName.endsWith(".segment") ? segmentName.substring(0, segmentName.length() - 8) : segmentName;
    }

    private PhaseSegmentGroup getOrCreateGroup(String baseName) {
        return segmentGroups.computeIfAbsent(baseName,
                name -> new PhaseSegmentGroup(name, rootDir.resolve("segments"), compactor));
    }

    private void registerSegment(SegmentWriter w) {
        manifest.registerSegmentIfAbsent(w.getSegmentName());
        getOrCreateGroup(base(w.getSegmentName())).registerIfAbsent(w);
        readerCache.updateVersion(w.getSegmentName(), w.getWriteOffset());
    }

    private void loadAllWritersFromManifest() {
        for (String seg : manifest.getAllSegmentNames()) {
            PhaseSegmentGroup g = getOrCreateGroup(base(seg));
            SegmentWriter w = g.containsSegment(seg)
                    ? g.getAll().stream().filter(sw -> sw.getSegmentName().equals(seg)).findFirst().orElseThrow()
                    : new SegmentWriter(rootDir.resolve("segments").resolve(seg));
            registerSegment(w);
        }
    }

    private Stream<SegmentWriter> getAllWritersStream() {
        return segmentGroups.values().stream()
                .flatMap(group -> {
                    Stream<SegmentWriter> base = group.getAll().stream();
                    SegmentWriter w = group.getWritable();
                    return (w == null) ? base : Stream.concat(base, Stream.of(w));
                })
                .distinct();
    }

    private int computePhaseBucket(WavePattern psi) {
        double phaseCenter = Arrays.stream(psi.phase()).average().orElse(0.0);
        return (int) Math.floor((phaseCenter + Math.PI) / BUCKET_WIDTH_RAD);
    }

    public PhaseShardSelector getShardSelector() {
        return shardSelectorRef.get();
    }

    public void compactPhase(String baseName) {
        PhaseSegmentGroup group = segmentGroups.get(baseName);
        if (group != null && group.maybeCompact()) rebuildShardSelector();
    }

    public boolean containsExactPattern(WavePattern pattern) {
        return manifest.contains(HashingUtil.computeContentHash(pattern));
    }

    private SegmentWriteResult writeToSegment(String id, WavePattern psi, PhaseSegmentGroup group) {
        SegmentWriter writer = group.getWritable();
        if (writer == null || writer.willOverflow(psi)) {
            writer = group.createAndRegisterNewSegment();
        }
        group.registerIfAbsent(writer);

        while (true) {
            try {
                long offset = writer.write(id, psi);
                long version = writer.flush();
                writer.sync();
                registerSegment(writer);
                return new SegmentWriteResult(writer, offset, version);
            } catch (SegmentOverflowException _e) {
                writer = group.createAndRegisterNewSegment();
                group.registerIfAbsent(writer);
            }
        }
    }

    private <T> List<T> collectTopK(SegmentWriter writer, int topK,
                                    Function<CachedReader, Stream<T>> supplier,
                                    Comparator<? super T> comparator) {
        CachedReader reader = readerCache.get(writer.getSegmentName());
        if (reader == null) return List.of();

        reader.acquire();
        try {
            PriorityQueue<T> heap = new PriorityQueue<>(topK, comparator);
            try (Stream<T> stream = supplier.apply(reader)) {
                stream.forEach(item -> {
                    if (heap.size() < topK) {
                        heap.add(item);
                    } else if (comparator.compare(item, heap.peek()) > 0) {
                        heap.poll();
                        heap.add(item);
                    }
                });
            }
            return new ArrayList<>(heap);
        } catch (IllegalStateException e) {
            return List.of();
        } finally {
            reader.release();
        }
    }

    private <T> List<T> deduplicateTopK(List<T> items, Function<T, String> idExtractor, Comparator<? super T> order,
                                        int topK) {
        Map<String, T> best = new HashMap<>();
        for (T item : items) {
            String id = idExtractor.apply(item);
            best.merge(id, item, (a, b) -> order.compare(a, b) >= 0 ? a : b);
        }
        return best.values().stream().sorted(order).limit(topK).toList();
    }

    private void shutdownExecutors() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) executor.shutdownNow();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) scheduler.shutdownNow();
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (flushDispatcher != null) flushDispatcher.close();
    }

    private void safeClose(SegmentWriter writer) {
        try { writer.close(); }
        catch (Exception e) { System.err.println("Failed to close writer: " + writer.getSegmentName()); }
    }

    @Override
    public void close() {
        try (AutoLock ignored = AutoLock.write(globalLock)) {
            shutdownExecutors();
            readerCache.close();
            segmentGroups.values().forEach(group -> group.getAll().forEach(this::safeClose));
            manifest.flush();
            metaStore.flush();
        }
    }

    private abstract static class QueryTask<T> extends RecursiveTask<List<T>> {
        final List<SegmentWriter> writers;
        private final int from;
        private final int to;
        private final int threshold;

        protected QueryTask(List<SegmentWriter> writers, int from, int to, int threshold) {
            this.writers = writers;
            this.from = from;
            this.to = to;
            this.threshold = threshold;
        }

        protected abstract List<T> process(SegmentWriter writer);

        @Override
        protected List<T> compute() {
            if (to - from <= threshold) {
                List<T> results = new ArrayList<>();
                for (int i = from; i < to; i++) {
                    results.addAll(process(writers.get(i)));
                }
                return results;
            } else {
                int mid = (from + to) >>> 1;
                QueryTask<T> left = cloneFor(from, mid);
                QueryTask<T> right = cloneFor(mid, to);
                left.fork();
                List<T> rightResult = right.compute();
                List<T> leftResult = left.join();
                leftResult.addAll(rightResult);
                return leftResult;
            }
        }

        protected abstract QueryTask<T> cloneFor(int from, int to);
    }

    private class MatchQueryTask extends QueryTask<HeapItem> {
        private final WavePattern query;
        private final String queryId;
        private final int topK;

        public MatchQueryTask(List<SegmentWriter> writers, WavePattern query, String queryId, int topK, int from, int to) {
            super(writers, from, to, 1);
            this.query = query;
            this.queryId = queryId;
            this.topK = topK;
        }

        @Override
        protected List<HeapItem> process(SegmentWriter writer) {
            return collectMatchesFromWriter(writer, query, queryId, topK);
        }

        @Override
        protected QueryTask<HeapItem> cloneFor(int from, int to) {
            return new MatchQueryTask(this.writers, query, queryId, topK, from, to);
        }

    }

    private class DetailedMatchQueryTask extends QueryTask<HeapItemDetailed> {
        private final WavePattern query;
        private final String queryId;
        private final int topK;

        public DetailedMatchQueryTask(List<SegmentWriter> writers, WavePattern query, String queryId, int topK, int from, int to) {
            super(writers, from, to, 1);
            this.query = query;
            this.queryId = queryId;
            this.topK = topK;
        }

        @Override
        protected List<HeapItemDetailed> process(SegmentWriter writer) {
            return collectDetailedFromWriter(writer, query, queryId, topK);
        }

        @Override
        protected QueryTask<HeapItemDetailed> cloneFor(int from, int to) {
            return new DetailedMatchQueryTask(this.writers, query, queryId, topK, from, to);
        }
    }
}