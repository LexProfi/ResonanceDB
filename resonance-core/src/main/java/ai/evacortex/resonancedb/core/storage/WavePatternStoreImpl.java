/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.*;
import ai.evacortex.resonancedb.core.engine.*;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings("resource")
public class WavePatternStoreImpl implements ResonanceStore, Closeable {

    private static final int BUCKETS =
            Integer.getInteger("resonance.segment.buckets", 64);

    private static final double BUCKET_WIDTH_RAD = (BUCKETS > 0)
            ? (2 * Math.PI) / BUCKETS
            : Double.parseDouble(System.getProperty("resonance.segment.bucketRad", "0.2"));

    private static final int PHASE_NEIGHBORS_MAX =
            Integer.getInteger("resonance.phase.neighbors.max",
                    Math.max(8, (int) Math.ceil(Math.PI / BUCKET_WIDTH_RAD)));

    private static final int BATCH_SIZE_BASE =
            Integer.getInteger("resonance.query.batchSize", 1024);

    private static final int OVERFETCH_FACTOR_BASE =
            Integer.getInteger("resonance.query.overfetch", 4);

    private static final boolean FLUSH_ASYNC =
            Boolean.parseBoolean(System.getProperty("resonance.flush.async", "false"));

    private static final float EXACT_MATCH_EPS = 1e-6f;
    private static final double READ_EPSILON = 0.1;

    private final Path rootDir;
    private final ManifestIndex manifest;
    private final PatternMetaStore metaStore;
    private final Map<String, PhaseSegmentGroup> segmentGroups;

    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();
    private final AtomicReference<PhaseShardSelector> shardSelectorRef;

    private final SegmentCache readerCache;
    private final SegmentCompactor compactor;
    private final ResonanceTracer tracer;

    private final ForkJoinPool queryPool;
    private final ScheduledExecutorService scheduler;
    private final FlushDispatcher flushDispatcher;
    private final ResonanceKernel resonanceKernel;

    private final java.lang.reflect.Method compareManyFlatMethod;

    private final Semaphore ioLimiter;
    private final int ioPermitsMaxBound;
    private final AtomicInteger ioPermitsCurrent = new AtomicInteger(1);
    private final java.util.concurrent.atomic.LongAdder ioAcquireCount = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder ioWaitNanos   = new java.util.concurrent.atomic.LongAdder();
    private volatile long lastRebalanceNanos = System.nanoTime();

    private record HeapItem(ResonanceMatch match, float priority) {}
    private record HeapItemDetailed(ResonanceMatchDetailed match, double priority) {}
    private record SegmentWriteResult(SegmentWriter writer, long offset, long version) {}

    private static final class Adaptive {
        final long maxHeap;
        final int hwThreads;
        final int poolParallelism;
        final int ioMaxPermits;

        Adaptive() {
            Runtime rt = Runtime.getRuntime();
            this.maxHeap = rt.maxMemory();
            this.hwThreads = Math.max(1, Runtime.getRuntime().availableProcessors());

            int base = hwThreads;
            if (maxHeap < (768L << 20)) {
                base = Math.max(1, (int) Math.ceil(hwThreads * 0.75));
            } else if (maxHeap < (1536L << 20)) {
                base = Math.max(1, (int) Math.ceil(hwThreads * 0.9));
            }
            this.poolParallelism = Math.min(Math.max(1, base), hwThreads * 4);

            int ioBase = Math.max(1, hwThreads / 2);
            if (maxHeap < (768L << 20)) ioBase = Math.max(1, hwThreads / 3);
            this.ioMaxPermits = Math.max(1, ioBase);
        }

        long estimatePatternBytes(int len) {
            long arrays = (long) len * 16L;
            long overhead = 128L;
            return arrays + overhead;
        }

        int batchSizeForLen(int len, int concurrentTasks) {
            final long perPat = Math.max(estimatePatternBytes(len), 1024L);
            final long heap = this.maxHeap;

            long targetBytes;
            if (heap < (768L << 20)) {
                targetBytes = (long) (1.5 * (1 << 20));
            } else if (heap < (1536L << 20)) {
                targetBytes = 3L << 20;
            } else {
                targetBytes = 6L << 20;
            }

            if (concurrentTasks <= hwThreads / 2) {
                targetBytes = (long) Math.min(targetBytes * 1.5, 8L << 20);
            }

            long bs = Math.max(64L, Math.min(BATCH_SIZE_BASE, targetBytes / perPat));
            if (concurrentTasks > hwThreads) {
                bs = Math.max(64L, (long) (bs * 0.75));
            }
            return (int) Math.max(64L, Math.min(4096L, bs));
        }

        int overfetchForTopK(int topK) {
            if (topK <= 5)  return Math.max(OVERFETCH_FACTOR_BASE, 4);
            if (topK <= 10) return Math.max(3, OVERFETCH_FACTOR_BASE - 1);
            if (topK <= 50) return Math.max(2, OVERFETCH_FACTOR_BASE - 2);
            return 2;
        }
    }

    private final Adaptive tune = new Adaptive();

    private static final ThreadLocal<FlatBuffers> TL_FLAT = ThreadLocal.withInitial(FlatBuffers::new);

    private static final class FlatBuffers {
        double[] ampFlat;
        double[] phaseFlat;
        String[] ids;
        float[] scores;
        void ensure(int len, int batch) {
            int need = len * batch;
            if (ampFlat == null || ampFlat.length < need) ampFlat = new double[need];
            if (phaseFlat == null || phaseFlat.length < need) phaseFlat = new double[need];
            if (ids == null || ids.length < batch) ids = new String[batch];
            if (scores == null || scores.length < batch) scores = new float[batch];
        }
    }

    public WavePatternStoreImpl(Path dbRoot) {
        this.rootDir = dbRoot;
        this.manifest = ManifestIndex.loadOrCreate(dbRoot.resolve("index/manifest.idx"));
        this.manifest.ensureFileExists();
        this.metaStore = PatternMetaStore.loadOrCreate(dbRoot.resolve("metadata/pattern-meta.json"));
        this.readerCache = new SegmentCache(dbRoot.resolve("segments"));
        this.compactor = new DefaultSegmentCompactor(manifest, metaStore, rootDir.resolve("segments"), globalLock);
        this.segmentGroups = new ConcurrentHashMap<>();
        this.tracer = new NoOpTracer();

        this.queryPool = new ForkJoinPool(
                tune.poolParallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                false
        );

        this.ioPermitsMaxBound = Math.max(1, tune.ioMaxPermits);
        int startPermits = Math.max(1, Math.min(ioPermitsMaxBound, Math.max(1, tune.hwThreads / 3)));
        this.ioLimiter = new Semaphore(startPermits, true);
        this.ioPermitsCurrent.set(startPermits);

        this.scheduler = Executors.newScheduledThreadPool(1);

        loadAllWritersFromManifest();
        this.shardSelectorRef = new AtomicReference<>(createShardSelector());

        scheduler.scheduleAtFixedRate(() ->
                segmentGroups.keySet().forEach(this::compactPhase), 10, 5, TimeUnit.MINUTES);

        scheduler.scheduleAtFixedRate(this::maybeRebalanceIoScheduled, 2, 2, TimeUnit.SECONDS);

        this.flushDispatcher = FLUSH_ASYNC ? new FlushDispatcher(Duration.ofMillis(5)) : null;
        this.resonanceKernel = Boolean.getBoolean("resonance.kernel.native")
                ? new NativeKernel()
                : new JavaKernel();

        this.compareManyFlatMethod = resolveCompareManyFlat(resonanceKernel);
    }

    private static java.lang.reflect.Method resolveCompareManyFlat(Object kernel) {
        try {
            return kernel.getClass().getMethod(
                    "compareManyFlat",
                    double[].class, double[].class, double[].class, double[].class, int.class, int.class
            );
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    @Override
    public String insert(WavePattern psi, Map<String, String> metadata)
            throws DuplicatePatternException, InvalidWavePatternException {

        Objects.requireNonNull(psi, "psi must not be null");
        if (psi.amplitude().length != psi.phase().length) {
            throw new InvalidWavePatternException("Amplitude / phase length mismatch");
        }

        String idKey = HashingUtil.computeContentHash(psi);
        HashingUtil.parseAndValidateMd5(idKey);

        try (AutoLock ignored = AutoLock.write(globalLock)) {
            if (manifest.contains(idKey)) throw new DuplicatePatternException(idKey);

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
            try { manifest.remove(idKey); } catch (Throwable ignored2) {}
            try { metaStore.remove(idKey); } catch (Throwable ignored3) {}
            throw new RuntimeException("Insert failed: " + idKey, e);
        }
    }

    @Override
    public void delete(String idKey) throws PatternNotFoundException {
        Objects.requireNonNull(idKey, "idKey must not be null");
        HashingUtil.parseAndValidateMd5(idKey);

        try (AutoLock ignored = AutoLock.write(globalLock)) {
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
            throws PatternNotFoundException, InvalidWavePatternException, DuplicatePatternException {

        Objects.requireNonNull(newPattern, "newPattern must not be null");
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

            SegmentWriteResult result = writeToSegment(newId, newPattern, group);

            try {
                if (!oldLoc.segmentName().equals(result.writer().getSegmentName())
                        || oldLoc.offset() != result.offset()) {

                    SegmentWriter oldWriter = getOrCreateWriter(oldLoc.segmentName());
                    oldWriter.markDeleted(oldLoc.offset());
                    long ov = oldWriter.flush();
                    oldWriter.sync();
                    readerCache.updateVersion(oldWriter.getSegmentName(), ov);
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

                rebuildShardSelector();
                return newId;

            } catch (Exception rollbackEx) {
                try {
                    SegmentWriter w = result.writer();
                    w.markDeleted(result.offset());
                    long nv = w.flush();
                    w.sync();
                    readerCache.updateVersion(w.getSegmentName(), nv);
                } catch (Exception ignored2) {
                    System.err.println("Failed to rollback written pattern " + newId);
                }
                throw new RuntimeException("Replace failed after write: " + newId, rollbackEx);
            }
        }
    }

    @Override
    public List<ResonanceMatch> query(WavePattern query, int topK) {
        Objects.requireNonNull(query, "query must not be null");
        if (topK <= 0) return List.of();

        try (var ignored = AutoLock.read(globalLock)) {
            String queryId = HashingUtil.computeContentHash(query);

            Comparator<HeapItem> order = Comparator
                    .comparingDouble(HeapItem::priority).reversed()
                    .thenComparing((HeapItem h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<SegmentWriter> writers = selectWritersForQuery(query);

            int threshold = Math.max(4, writers.size() / Math.max(1, tune.poolParallelism));
            List<HeapItem> collected =
                    queryPool.invoke(new MatchQueryTask(writers, query, queryId, topK, 0, writers.size(), threshold));

            if (collected.size() < topK) {
                Set<String> seen = new HashSet<>();
                for (SegmentWriter w : writers) seen.add(w.getSegmentName());
                List<SegmentWriter> rest = getAllWritersStream()
                        .filter(w -> !seen.contains(w.getSegmentName()))
                        .toList();
                if (!rest.isEmpty()) {
                    List<HeapItem> extra =
                            queryPool.invoke(new MatchQueryTask(rest, query, queryId, topK, 0, rest.size(), threshold));
                    collected.addAll(extra);
                }
            }

            List<ResonanceMatch> prelim = deduplicateTopK(collected, h -> h.match().id(), order, topK)
                    .stream()
                    .map(HeapItem::match)
                    .toList();

            return materializePatterns(prelim);
        }
    }

    @Override
    public List<ResonanceMatchDetailed> queryDetailed(WavePattern query, int topK) {
        Objects.requireNonNull(query, "query must not be null");
        if (topK <= 0) return List.of();

        try (var ignored = AutoLock.read(globalLock)) {
            String queryId = HashingUtil.computeContentHash(query);

            Comparator<HeapItemDetailed> order = Comparator
                    .comparingDouble(HeapItemDetailed::priority).reversed()
                    .thenComparing((HeapItemDetailed h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<SegmentWriter> writers = selectWritersForQuery(query);
            int threshold = Math.max(4, writers.size() / Math.max(1, tune.poolParallelism));

            List<HeapItemDetailed> collected =
                    queryPool.invoke(new DetailedMatchQueryTask(writers, query, queryId, topK, 0, writers.size(), threshold));

            if (collected.size() < topK) {
                Set<String> seen = new HashSet<>();
                for (SegmentWriter w : writers) seen.add(w.getSegmentName());
                List<SegmentWriter> rest = getAllWritersStream()
                        .filter(w -> !seen.contains(w.getSegmentName()))
                        .toList();
                if (!rest.isEmpty()) {
                    List<HeapItemDetailed> extra =
                            queryPool.invoke(new DetailedMatchQueryTask(rest, query, queryId, topK, 0, rest.size(), threshold));
                    collected.addAll(extra);
                }
            }

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
    public List<ResonanceMatchDetailed> queryCompositeDetailed(List<WavePattern> patterns, List<Double> weights, int topK) {
        Objects.requireNonNull(patterns, "patterns must not be null");
        if (patterns.isEmpty()) throw new InvalidWavePatternException("Empty pattern list in composite query");
        WavePattern superposed = WavePatternUtils.superpose(patterns, weights);
        return queryDetailed(superposed, topK);
    }

    @Override
    public float compare(WavePattern a, WavePattern b) {
        return resonanceKernel.compare(a, b);
    }

    private List<HeapItem> collectMatchesFromWriter(SegmentWriter writer, WavePattern query, String queryId, int topK) {
        if (writer == null) return List.of();

        final Comparator<HeapItem> cmp = Comparator.comparingDouble(HeapItem::priority);
        final CachedReader reader = readerCache.get(writer.getSegmentName());
        if (reader == null) return List.of();

        final int len = query.amplitude().length;
        final int batchSize = tune.batchSizeForLen(len, activeTasksEstimate());
        final int localCap = Math.max(topK, 8);

        reader.acquire();
        try {
            final PriorityQueue<HeapItem> heap = new PriorityQueue<>(localCap, cmp);
            final boolean useFlat = (compareManyFlatMethod != null);

            FlatBuffers fb = TL_FLAT.get();
            fb.ensure(len, batchSize);

            int inBatch = 0;

            for (String id : reader.allIds()) {
                fb.ids[inBatch++] = id;

                if (inBatch == batchSize) {
                    acquireIoPermitBatch();
                    try {
                        if (useFlat) {
                            int ready = fillFlatBatch(reader, fb, len, inBatch);
                            if (ready > 0) {
                                scoreAndMergeFlat(query, queryId, fb, len, ready, heap, cmp, topK);
                            }
                        } else {
                            List<String> idBatch = new ArrayList<>(inBatch);
                            List<WavePattern> candBatch = new ArrayList<>(inBatch);
                            int ready = fillObjectBatch(reader, fb.ids, inBatch, idBatch, candBatch, len);
                            if (ready > 0) {
                                scoreAndMergeObject(query, queryId, idBatch, candBatch, heap, cmp, topK);
                            }
                        }
                    } finally {
                        releaseIoPermitBatch();
                    }
                    inBatch = 0;
                }
            }

            if (inBatch > 0) {
                acquireIoPermitBatch();
                try {
                    if (useFlat) {
                        int ready = fillFlatBatch(reader, fb, len, inBatch);
                        if (ready > 0) {
                            scoreAndMergeFlat(query, queryId, fb, len, ready, heap, cmp, topK);
                        }
                    } else {
                        List<String> idBatch = new ArrayList<>(inBatch);
                        List<WavePattern> candBatch = new ArrayList<>(inBatch);
                        int ready = fillObjectBatch(reader, fb.ids, inBatch, idBatch, candBatch, len);
                        if (ready > 0) {
                            scoreAndMergeObject(query, queryId, idBatch, candBatch, heap, cmp, topK);
                        }
                    }
                } finally {
                    releaseIoPermitBatch();
                }
            }

            return new ArrayList<>(heap);
        } finally {
            reader.release();
        }
    }

    private List<HeapItemDetailed> collectDetailedFromWriter(
            SegmentWriter writer, WavePattern query, String queryId, int topK) {

        if (writer == null) return List.of();

        final Comparator<HeapItemDetailed> cmp = Comparator.comparingDouble(HeapItemDetailed::priority);
        final CachedReader reader = readerCache.get(writer.getSegmentName());
        if (reader == null) return List.of();

        final int len = query.amplitude().length;
        final int overfetch = tune.overfetchForTopK(topK);
        final int localCap = Math.max(Math.max(32, topK), topK * overfetch);

        final PriorityQueue<HeapItemDetailed> heap = new PriorityQueue<>(localCap, cmp);

        reader.acquire();
        acquireIoPermitBatch();
        try {
            for (String id : reader.allIds()) {
                WavePattern cand = readNoSemaphore(reader, id);
                if (cand == null || cand.amplitude().length != len) continue;

                ComparisonResult result = resonanceKernel.compareWithPhaseDelta(query, cand);
                float energy = result.energy();
                double phaseShift = result.phaseDelta();
                ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
                double zoneScore = switch (zone) {
                    case CORE -> 2.0;
                    case FRINGE -> 1.0;
                    case SHADOW -> 0.0;
                };

                boolean idEq = id.equals(queryId);
                boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
                double priority = zoneScore + energy + (idEq ? 1.0 : 0.0) + (exactEq ? 0.5 : 0.0);

                HeapItemDetailed item = new HeapItemDetailed(
                        new ResonanceMatchDetailed(id, energy, cand, phaseShift, zone, zoneScore),
                        priority
                );

                if (heap.size() < localCap) {
                    heap.add(item);
                } else if (cmp.compare(item, heap.peek()) > 0) {
                    heap.poll();
                    heap.add(item);
                }
            }
            return new ArrayList<>(heap);
        } finally {
            releaseIoPermitBatch();
            reader.release();
        }
    }

    private int fillFlatBatch(CachedReader reader, FlatBuffers fb, int len, int count) {
        int ready = 0;
        for (int i = 0; i < count; i++) {
            String id = fb.ids[i];
            WavePattern cand = readNoSemaphore(reader, id);
            if (cand == null || cand.amplitude().length != len) continue;

            System.arraycopy(cand.amplitude(), 0, fb.ampFlat, ready * len, len);
            System.arraycopy(cand.phase(),     0, fb.phaseFlat, ready * len, len);
            fb.ids[ready] = id;
            ready++;
        }
        return ready;
    }

    private int fillObjectBatch(CachedReader reader,
                                String[] idsSrc, int count,
                                List<String> idsOut,
                                List<WavePattern> candsOut,
                                int len) {
        int ready = 0;
        for (int i = 0; i < count; i++) {
            String id = idsSrc[i];
            WavePattern cand = readNoSemaphore(reader, id);
            if (cand == null || cand.amplitude().length != len) continue;
            idsOut.add(id);
            candsOut.add(cand);
            ready++;
        }
        return ready;
    }

    private void scoreAndMergeObject(
            WavePattern query, String queryId,
            List<String> ids, List<WavePattern> cands,
            PriorityQueue<HeapItem> heap, Comparator<HeapItem> cmp, int topK) {

        float[] scores = resonanceKernel.compareMany(query, cands);
        for (int i = 0; i < scores.length; i++) {
            String id = ids.get(i);
            WavePattern cand = cands.get(i);
            float energy = scores[i];

            boolean idEq = id.equals(queryId);
            boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
            float priority = energy + (idEq ? 1.0f : 0.0f) + (exactEq ? 0.5f : 0.0f);

            tracer.trace(id, query, cand, energy);
            HeapItem item = new HeapItem(new ResonanceMatch(id, energy, cand), priority);

            if (heap.size() < topK) {
                heap.add(item);
            } else if (cmp.compare(item, heap.peek()) > 0) {
                heap.poll();
                heap.add(item);
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    private void scoreAndMergeFlat(
            WavePattern query, String queryId,
            FlatBuffers fb,
            int len, int count,
            PriorityQueue<HeapItem> heap, Comparator<HeapItem> cmp, int topK) {

        float[] scores;
        try {
            Object res = compareManyFlatMethod.invoke(
                    resonanceKernel,
                    query.amplitude(), query.phase(),
                    fb.ampFlat, fb.phaseFlat,
                    len, count
            );
            scores = (float[]) res;
        } catch (ReflectiveOperationException e) {

            List<WavePattern> cands = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                double[] a = Arrays.copyOfRange(fb.ampFlat,   i * len, (i + 1) * len);
                double[] p = Arrays.copyOfRange(fb.phaseFlat, i * len, (i + 1) * len);
                cands.add(new WavePattern(a, p));
            }
            scores = resonanceKernel.compareMany(query, cands);
        }

        for (int i = 0; i < count; i++) {
            String id = fb.ids[i];
            float energy = scores[i];

            boolean idEq = id.equals(queryId);
            boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
            float priority = energy + (idEq ? 1.0f : 0.0f) + (exactEq ? 0.5f : 0.0f);

            HeapItem item = new HeapItem(new ResonanceMatch(id, energy, null), priority);

            if (heap.size() < topK) {
                heap.add(item);
            } else if (cmp.compare(item, heap.peek()) > 0) {
                heap.poll();
                heap.add(item);
            }
        }
    }

    private List<SegmentWriter> selectWritersForQuery(WavePattern query) {
        PhaseShardSelector selector = shardSelectorRef.get();

        double eps = READ_EPSILON;
        List<String> shardNames = selector.getRelevantShards(query, eps);

        int tries = 0;
        while (shardNames.isEmpty() && tries < PHASE_NEIGHBORS_MAX) {
            eps += BUCKET_WIDTH_RAD;
            shardNames = selector.getRelevantShards(query, eps);
            tries++;
        }

        Set<String> allowed = new LinkedHashSet<>(shardNames);
        List<SegmentWriter> writers = getAllWritersStream()
                .filter(w -> allowed.contains(w.getSegmentName()))
                .toList();

        if (writers.isEmpty()) {
            writers = getAllWritersStream().toList();
        }
        return writers;
    }

    private SegmentWriter getOrCreateWriter(String segmentName) {
        String groupKey = base(segmentName);

        PhaseSegmentGroup group = segmentGroups.computeIfAbsent(groupKey,
                key -> new PhaseSegmentGroup(key, rootDir.resolve("segments"), this.compactor)
        );

        Optional<SegmentWriter> existing = group.getAll().stream()
                .filter(sw -> sw.getSegmentName().equals(segmentName))
                .findFirst();

        if (existing.isPresent()) return existing.get();

        SegmentWriter w = new SegmentWriter(rootDir.resolve("segments").resolve(segmentName));
        registerSegment(w);
        return w;
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
        if (dash < 0) {
            return segmentName.endsWith(".segment")
                    ? segmentName.substring(0, segmentName.length() - 8)
                    : segmentName;
        }
        int dot = segmentName.indexOf(".segment");
        int nextDash = segmentName.indexOf('-', dash + 1);
        int end = (nextDash > 0) ? nextDash : (dot > 0 ? dot : segmentName.length());
        return segmentName.substring(0, end);
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
            SegmentWriter w = g.getAll().stream()
                    .filter(sw -> sw.getSegmentName().equals(seg))
                    .findFirst()
                    .orElseGet(() -> new SegmentWriter(rootDir.resolve("segments").resolve(seg)));
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
        return normBucketIndex((int) Math.floor((phaseCenter + Math.PI) / BUCKET_WIDTH_RAD));
    }

    private int normBucketIndex(int idx) {
        int maxIdx = maxBucketIndex();
        if (idx < 0) return 0;
        if (idx > maxIdx) return maxIdx;
        return idx;
    }

    private int maxBucketIndex() {
        return (int) Math.ceil((2 * Math.PI) / BUCKET_WIDTH_RAD) - 1;
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
            } catch (SegmentOverflowException ignored) {
                writer = group.createAndRegisterNewSegment();
                group.registerIfAbsent(writer);
            }
        }
    }

    private <T> List<T> deduplicateTopK(
            List<T> items,
            Function<T, String> idExtractor,
            Comparator<? super T> order,
            int topK) {

        Map<String, T> best = new HashMap<>();
        for (T item : items) {
            String id = idExtractor.apply(item);
            best.merge(id, item, (a, b) -> order.compare(a, b) >= 0 ? a : b);
        }
        return best.values().stream().sorted(order).limit(topK).toList();
    }

    private void shutdownExecutors() {
        queryPool.shutdown();
        try {
            if (!queryPool.awaitTermination(30, TimeUnit.SECONDS)) queryPool.shutdownNow();
        } catch (InterruptedException e) {
            queryPool.shutdownNow();
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

    private void maybeRebalanceIoOnHotpath() {
        long cnt = ioAcquireCount.sum();
        if ((cnt & 0x7FFF) != 0) return;
        long now = System.nanoTime();
        if (now - lastRebalanceNanos < 200_000_000L) return;
        lastRebalanceNanos = now;
        rebalanceIoInternal(false);
    }

    private void maybeRebalanceIoScheduled() {
        rebalanceIoInternal(true);
    }

    private void rebalanceIoInternal(boolean scheduled) {
        final long count = ioAcquireCount.sumThenReset();
        final long wait = ioWaitNanos.sumThenReset();
        if (count == 0) return;

        double avgWaitMicros = (wait / (double) count) / 1000.0;

        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();
        long headroom = rt.maxMemory() - used;
        double headroomRatio = Math.max(0.0, headroom / (double) rt.maxMemory());

        int curr = ioPermitsCurrent.get();

        if (headroomRatio < 0.10 && curr > 1) {
            if (ioLimiter.tryAcquire(1)) {
                ioPermitsCurrent.addAndGet(-1);
            }
            return;
        }

        if (avgWaitMicros > 200.0 && curr < ioPermitsMaxBound) {
            ioLimiter.release(1);
            ioPermitsCurrent.addAndGet(1);
            return;
        }

        if (avgWaitMicros < 10.0 && headroomRatio < 0.25 && curr > 1) {
            if (ioLimiter.tryAcquire(1)) {
                ioPermitsCurrent.decrementAndGet();
            }
        }
    }

    private void acquireIoPermitBatch() {
        long start = System.nanoTime();
        try {
            ioLimiter.acquire();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            long waited = System.nanoTime() - start;
            ioAcquireCount.increment();
            ioWaitNanos.add(waited);
            maybeRebalanceIoOnHotpath();
        }
    }

    private void releaseIoPermitBatch() {
        ioLimiter.release();
    }

    private WavePattern readNoSemaphore(CachedReader reader, String id) {
        try {
            return reader.readById(id);
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private List<ResonanceMatch> materializePatterns(List<ResonanceMatch> matches) {
        boolean need = false;
        for (ResonanceMatch m : matches) {
            if (m.pattern() == null) { need = true; break; }
        }
        if (!need) return matches;

        List<ResonanceMatch> out = new ArrayList<>(matches.size());
        for (ResonanceMatch m : matches) {
            if (m.pattern() != null) {
                out.add(m);
                continue;
            }
            String id = m.id();
            WavePattern pat = null;
            ManifestIndex.PatternLocation loc = manifest.get(id);
            if (loc != null) {
                CachedReader r = readerCache.get(loc.segmentName());
                if (r != null) {
                    r.acquire();
                    try {
                        pat = readNoSemaphore(r, id);
                    } finally {
                        r.release();
                    }
                }
            }
            out.add(new ResonanceMatch(id, m.energy(), pat));
        }
        return out;
    }

    private int activeTasksEstimate() {
        int active = queryPool.getActiveThreadCount();
        if (active <= 0) active = Math.max(1, queryPool.getPoolSize());
        return active;
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
            this.threshold = Math.max(2, threshold);
        }

        protected abstract List<T> process(SegmentWriter writer);

        @Override
        protected List<T> compute() {
            int span = to - from;
            if (span <= threshold) {
                List<T> results = new ArrayList<>(span * 2);
                for (int i = from; i < to; i++) {
                    results.addAll(process(writers.get(i)));
                }
                return results;
            } else {
                int mid = (from + to) >>> 1;
                QueryTask<T> left = cloneFor(from, mid, threshold);
                QueryTask<T> right = cloneFor(mid, to, threshold);
                left.fork();
                List<T> rightResult = right.compute();
                List<T> leftResult = left.join();
                if (!rightResult.isEmpty()) leftResult.addAll(rightResult);
                return leftResult;
            }
        }

        protected abstract QueryTask<T> cloneFor(int from, int to, int threshold);
    }

    private class MatchQueryTask extends QueryTask<HeapItem> {
        private final WavePattern query;
        private final String queryId;
        private final int topK;

        public MatchQueryTask(List<SegmentWriter> writers, WavePattern query, String queryId, int topK, int from, int to, int threshold) {
            super(writers, from, to, threshold);
            this.query = query;
            this.queryId = queryId;
            this.topK = topK;
        }

        @Override
        protected List<HeapItem> process(SegmentWriter writer) {
            return collectMatchesFromWriter(writer, query, queryId, topK);
        }

        @Override
        protected QueryTask<HeapItem> cloneFor(int from, int to, int threshold) {
            return new MatchQueryTask(this.writers, query, queryId, topK, from, to, threshold);
        }
    }

    private class DetailedMatchQueryTask extends QueryTask<HeapItemDetailed> {
        private final WavePattern query;
        private final String queryId;
        private final int topK;

        public DetailedMatchQueryTask(List<SegmentWriter> writers, WavePattern query, String queryId, int topK, int from, int to, int threshold) {
            super(writers, from, to, threshold);
            this.query = query;
            this.queryId = queryId;
            this.topK = topK;
        }

        @Override
        protected List<HeapItemDetailed> process(SegmentWriter writer) {
            return collectDetailedFromWriter(writer, query, queryId, topK);
        }

        @Override
        protected QueryTask<HeapItemDetailed> cloneFor(int from, int to, int threshold) {
            return new DetailedMatchQueryTask(this.writers, query, queryId, topK, from, to, threshold);
        }
    }
}