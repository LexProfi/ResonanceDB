/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.ResonanceStore;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.exceptions.InvalidWavePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.exceptions.SegmentOverflowException;
import ai.evacortex.resonancedb.core.index.CandidateSource;
import ai.evacortex.resonancedb.core.index.DeltaIndex;
import ai.evacortex.resonancedb.core.index.FullScanCandidateSource;
import ai.evacortex.resonancedb.core.index.IvfCandidateSource;
import ai.evacortex.resonancedb.core.index.PostingSidecar;
import ai.evacortex.resonancedb.core.math.ResonanceZone;
import ai.evacortex.resonancedb.core.math.ResonanceZoneClassifier;
import ai.evacortex.resonancedb.core.math.WavePatternUtils;
import ai.evacortex.resonancedb.core.metadata.PatternMetaStore;
import ai.evacortex.resonancedb.core.sharding.PhaseShardSelector;
import ai.evacortex.resonancedb.core.storage.compactor.DefaultSegmentCompactor;
import ai.evacortex.resonancedb.core.storage.compactor.SegmentCompactor;
import ai.evacortex.resonancedb.core.storage.io.CachedReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentCache;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import ai.evacortex.resonancedb.core.storage.responce.ComparisonResult;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceEntry;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceMap;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;
import ai.evacortex.resonancedb.core.storage.util.AutoLock;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
import ai.evacortex.resonancedb.core.storage.util.NoOpTracer;

import ai.evacortex.resonancedb.core.storage.wal.WalCorruptionException;
import ai.evacortex.resonancedb.core.storage.wal.WalRecord;
import ai.evacortex.resonancedb.core.storage.wal.WriteAheadLog;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings("resource")
public class WavePatternStoreImpl implements ResonanceStore, Closeable {

    private static final int DEFAULT_PATTERN_LEN = 1536;
    private static final int BATCH_SIZE_BASE = Integer.getInteger("resonance.query.batchSize", 1024);
    private static final int OVERFETCH_FACTOR_BASE = Integer.getInteger("resonance.query.overfetch", 4);
    private static final double READ_EPSILON = 0.1;
    private static final float EXACT_MATCH_EPS = 1e-6f;

    private static final int BUCKETS =
            Integer.getInteger("resonance.segment.buckets", 64);

    private static final double BUCKET_WIDTH_RAD = (BUCKETS > 0)
            ? (2 * Math.PI) / BUCKETS
            : Double.parseDouble(System.getProperty("resonance.segment.bucketRad", "0.2"));

    private static final int PHASE_NEIGHBORS_MAX =
            Integer.getInteger("resonance.phase.neighbors.max",
                    Math.max(8, (int) Math.ceil(Math.PI / BUCKET_WIDTH_RAD)));

    private final int patternLen;
    private final Path rootDir;

    private final ManifestIndex manifest;
    private final PatternMetaStore metaStore;
    private final Map<String, PhaseSegmentGroup> segmentGroups;

    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();
    private final AtomicReference<PhaseShardSelector> shardSelectorRef;

    private final SegmentCache readerCache;
    private final SegmentCompactor compactor;
    private final ResonanceTracer tracer;

    private final StoreRuntimeServices runtime;
    private final boolean ownRuntime;
    private final ForkJoinPool queryPool;
    private final ResonanceKernel resonanceKernel;
    private final Method compareManyFlatMethod;

    private final ScheduledFuture<?> compactionTask;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final boolean indexEnabled;
    private final boolean exactEquivalence;
    private final IvfCandidateSource ivfSource;
    private final DeltaIndex deltaIndex;
    private final Path centroidsPath;
    private final int ivfNProbe;
    private final int ivfDeltaMaxSize;
    private final AtomicBoolean indexRebuildScheduled = new AtomicBoolean(false);
    private final Object rebuildMutex = new Object();
    private final AtomicReference<ScheduledFuture<?>> rebuildFuture = new AtomicReference<>();

    private final int batchCommitSize = Integer.getInteger("resonance.insert.batchSize", 1);
    private int pendingFlushCount = 0;
    private final Set<SegmentWriter> pendingWriters = new LinkedHashSet<>();

    private final boolean walEnabled;
    private final WriteAheadLog wal;
    private final DeltaBuffer deltaBuffer;
    private final int sealThreshold;
    private final int deltaMaxEntries;
    private final long backpressureTimeoutMs;
    private final AtomicBoolean sealScheduled = new AtomicBoolean(false);
    private final AtomicReference<ScheduledFuture<?>> sealFuture = new AtomicReference<>();
    private final Object sealMutex = new Object();
    private final WriteAheadLog.DurabilityMode walDurability;

    private static final int WAL_SYNC_THRESHOLD = 256;
    private final List<CompletableFuture<Long>> pendingWalFutures =
            Collections.synchronizedList(new ArrayList<>());

    private record HeapItem(ResonanceMatch match, boolean idMatch, boolean exactMatch) {}
    private record HeapItemDetailed(ResonanceMatchDetailed match, boolean idMatch, boolean exactMatch) {}
    private record SegmentWriteResult(SegmentWriter writer, long offset, long version) {}

    private static final Comparator<HeapItem> HEAP_ORDER = Comparator
            .comparing(HeapItem::idMatch)
            .thenComparing(HeapItem::exactMatch)
            .thenComparingDouble(h -> h.match().energy())
            .thenComparing(h -> h.match().id(), Comparator.reverseOrder());

    private static final Comparator<HeapItem> RESULT_ORDER = HEAP_ORDER.reversed();

    private static final Comparator<HeapItemDetailed> HEAP_ORDER_DETAILED = Comparator
            .comparing(HeapItemDetailed::idMatch)
            .thenComparing(HeapItemDetailed::exactMatch)
            .thenComparingDouble(h -> h.match().energy())
            .thenComparing(h -> h.match().id(), Comparator.reverseOrder());

    private static final Comparator<HeapItemDetailed> RESULT_ORDER_DETAILED = HEAP_ORDER_DETAILED.reversed();

    private static <T> void heapAddOrEvict(PriorityQueue<T> heap, T item,
                                            Comparator<T> heapOrder, int topK) {
        if (heap.size() < topK) {
            heap.add(item);
        } else if (heapOrder.compare(item, heap.peek()) > 0) {
            heap.poll();
            heap.add(item);
        }
    }

    private final Adaptive tune = new Adaptive();

    private static final ThreadLocal<FlatBuffers> TL_FLAT =
            ThreadLocal.withInitial(FlatBuffers::new);

    private static final class FlatBuffers {
        double[] ampFlat;
        double[] phaseFlat;
        String[] ids;

        void ensure(int len, int batch) {
            int need = len * batch;
            if (ampFlat == null || ampFlat.length < need) {
                ampFlat = new double[need];
            }
            if (phaseFlat == null || phaseFlat.length < need) {
                phaseFlat = new double[need];
            }
            if (ids == null || ids.length < batch) {
                ids = new String[batch];
            }
        }
    }

    private static final class Adaptive {
        final long maxHeap;
        final int hwThreads;
        final int poolParallelism;

        Adaptive() {
            Runtime rt = Runtime.getRuntime();
            this.maxHeap = rt.maxMemory();
            this.hwThreads = Math.max(1, rt.availableProcessors());

            int base = hwThreads;
            if (maxHeap < (768L << 20)) {
                base = Math.max(1, (int) Math.ceil(hwThreads * 0.75));
            } else if (maxHeap < (1536L << 20)) {
                base = Math.max(1, (int) Math.ceil(hwThreads * 0.9));
            }
            this.poolParallelism = Math.min(Math.max(1, base), hwThreads * 4);
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

    public WavePatternStoreImpl(Path dbRoot) {
        this(
                dbRoot,
                Integer.getInteger("resonance.pattern.len", DEFAULT_PATTERN_LEN),
                StoreRuntimeServices.fromSystemProperties(),
                true
        );
    }

    public WavePatternStoreImpl(Path dbRoot, StoreRuntimeServices runtime) {
        this(
                dbRoot,
                Integer.getInteger("resonance.pattern.len", DEFAULT_PATTERN_LEN),
                runtime,
                false
        );
    }

    public WavePatternStoreImpl(Path dbRoot, int patternLen, StoreRuntimeServices runtime) {
        this(dbRoot, patternLen, runtime, false);
    }

    private WavePatternStoreImpl(Path dbRoot,
                                 int patternLen,
                                 StoreRuntimeServices runtime,
                                 boolean ownRuntime) {
        Objects.requireNonNull(dbRoot, "dbRoot must not be null");
        Objects.requireNonNull(runtime, "runtime must not be null");

        if (patternLen <= 0) {
            throw new IllegalArgumentException("patternLen must be > 0, got: " + patternLen);
        }

        this.patternLen = patternLen;
        this.rootDir = dbRoot.toAbsolutePath().normalize();
        this.runtime = runtime;
        this.ownRuntime = ownRuntime;

        this.manifest = ManifestIndex.loadOrCreate(this.rootDir.resolve("index/manifest.idx"));
        this.manifest.ensureFileExists();
        this.metaStore = PatternMetaStore.loadOrCreate(this.rootDir.resolve("metadata/pattern-meta.json"));
        this.readerCache = new SegmentCache(this.rootDir.resolve("segments"));
        this.compactor = new DefaultSegmentCompactor(
                manifest,
                metaStore,
                this.rootDir.resolve("segments"),
                globalLock
        );
        this.segmentGroups = new ConcurrentHashMap<>();
        this.tracer = new NoOpTracer();

        this.queryPool = runtime.queryPool();
        this.resonanceKernel = runtime.resonanceKernel();
        this.compareManyFlatMethod = resolveCompareManyFlat(resonanceKernel);

        loadAllWritersFromManifest();
        verifyPatternLengthOnOpen();
        this.shardSelectorRef = new AtomicReference<>(createShardSelector());

        this.compactionTask = runtime.scheduler().scheduleAtFixedRate(
                this::safeCompactSweep,
                10, 5, TimeUnit.MINUTES
        );

        this.walEnabled = Boolean.parseBoolean(
                System.getProperty("resonance.wal.enabled", "false"));
        this.sealThreshold = Integer.getInteger("resonance.delta.sealThreshold", 5000);
        this.deltaMaxEntries = Integer.getInteger("resonance.delta.maxEntries", 20_000);
        this.backpressureTimeoutMs = Long.getLong("resonance.delta.backpressureTimeoutMs", 10_000);

        String durStr = System.getProperty("resonance.wal.durability", "group");
        this.walDurability = switch (durStr.toLowerCase()) {
            case "strict" -> WriteAheadLog.DurabilityMode.STRICT;
            case "async" -> WriteAheadLog.DurabilityMode.ASYNC;
            default -> WriteAheadLog.DurabilityMode.GROUP;
        };

        if (this.walEnabled) {
            int groupSize = Integer.getInteger("resonance.wal.groupSize", 256);
            long groupIntervalMs = Long.getLong("resonance.wal.groupIntervalMs", 2);
            long segmentBytes = Long.getLong("resonance.wal.segmentBytes", 128L << 20);
            Path walDir = this.rootDir.resolve(
                    System.getProperty("resonance.wal.dir", "wal"));

            try {
                this.wal = new WriteAheadLog(walDir, walDurability,
                        groupSize, groupIntervalMs, segmentBytes);
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize WAL: " + e.getMessage(), e);
            }
            this.deltaBuffer = new DeltaBuffer(patternLen);

            try {
                List<WalRecord> walRecords = WriteAheadLog.replay(walDir);
                for (WalRecord rec : walRecords) {
                    replayWalRecord(rec);
                }
                if (!walRecords.isEmpty()) {
                    System.err.println("WAL replay: applied " + walRecords.size() +
                            " records, deltaBuffer size=" + deltaBuffer.size());
                }
            } catch (WalCorruptionException e) {
                throw new RuntimeException("WAL replay failed — non-tail corruption: " +
                        e.getMessage(), e);
            } catch (IOException e) {
                throw new RuntimeException("WAL replay I/O error: " + e.getMessage(), e);
            }
        } else {
            this.wal = null;
            this.deltaBuffer = null;
        }

        this.indexEnabled = Boolean.parseBoolean(
                System.getProperty("resonance.index.enabled", "false"));
        this.exactEquivalence = Boolean.parseBoolean(
                System.getProperty("resonance.index.exactEquivalence", "true"));
        this.ivfNProbe = Integer.getInteger("resonance.index.l1.nprobe", 8);
        this.ivfDeltaMaxSize = Integer.getInteger("resonance.index.delta.maxSize", 10_000);

        if (this.indexEnabled) {
            this.deltaIndex = new DeltaIndex();
            this.centroidsPath = this.rootDir.resolve("index/centroids.bin");

            FullScanCandidateSource fallback = new FullScanCandidateSource(
                    manifest::getAllSegmentNames,
                    segName -> readerCache.getOrLoad(segName)
            );
            this.ivfSource = new IvfCandidateSource(
                    null, deltaIndex, manifest, ivfNProbe, fallback);
            this.ivfSource.setSidecarPath(this.rootDir.resolve("index/postings.ivf"));

            if (!ivfSource.loadIndex(centroidsPath)) {
                scheduleIndexRebuild();
            } else {
                repopulatePostings();
                if (!ivfSource.loadSidecar()) {
                    rebuildSidecar();
                }
            }
        } else {
            this.deltaIndex = null;
            this.ivfSource = null;
            this.centroidsPath = null;
        }
    }

    @Override
    public String insert(WavePattern psi, Map<String, String> metadata)
            throws DuplicatePatternException, InvalidWavePatternException {

        ensureOpen();
        validateWavePatternLen(psi);
        Map<String, String> safeMetadata = metadata == null ? Map.of() : metadata;

        String idKey = HashingUtil.computeContentHash(psi);
        byte[] idBytes = HashingUtil.parseAndValidateMd5(idKey);

        if (deltaBuffer != null) {
            return insertViaDelta(idKey, idBytes, psi, safeMetadata);
        }

        return insertLegacy(idKey, idBytes, psi, safeMetadata);
    }

    private String insertViaDelta(String idKey, byte[] idBytes,
                                   WavePattern psi, Map<String, String> metadata) {
        double phaseCenter = meanPhase(psi.phase());

        if (deltaBuffer.size() >= deltaMaxEntries) {
            awaitSealCompletion();
            if (deltaBuffer.size() >= deltaMaxEntries) {
                throw new RuntimeException("DeltaBuffer full after backpressure timeout (" +
                        deltaMaxEntries + " entries). Increase delta.maxEntries or reduce insert rate.");
            }
        }

        long lsn = wal.nextLsn();
        WalRecord walRecord = WalRecord.insert(lsn, idKey, metadata, psi);
        CompletableFuture<Long> durabilityFuture = wal.append(walRecord);

        try (AutoLock ignored = AutoLock.write(globalLock)) {
            if (manifest.contains(idKey) || deltaBuffer.contains(idKey)) {
                throw new DuplicatePatternException(idKey);
            }
            deltaBuffer.add(idKey, psi, metadata, phaseCenter, idBytes, lsn);
        }

        switch (walDurability) {
            case STRICT -> durabilityFuture.join();
            case GROUP -> {
                pendingWalFutures.add(durabilityFuture);
                if (pendingWalFutures.size() >= WAL_SYNC_THRESHOLD) {
                    flushWalFutures();
                }
            }
            case ASYNC -> {}
        }

        if (deltaBuffer.activeSize() >= sealThreshold) {
            scheduleSeal();
        }

        return idKey;
    }

    private String insertLegacy(String idKey, byte[] idBytes,
                                WavePattern psi, Map<String, String> metadata) {
        try (AutoLock ignored = AutoLock.write(globalLock)) {
            if (manifest.contains(idKey)) {
                throw new DuplicatePatternException(idKey);
            }

            double phaseCenter = meanPhase(psi.phase());
            int bucket = computePhaseBucket(phaseCenter);
            String base = "phase-" + bucket;

            PhaseSegmentGroup group = getOrCreateGroup(base);
            double existingCenter = group.getAvgPhase();

            boolean phaseOverflow = Math.abs(phaseCenter - existingCenter) > 0.15;
            SegmentWriter writer = group.getWritable();
            if (writer == null || writer.willOverflow(psi) || phaseOverflow) {
                writer = group.createAndRegisterNewSegment();
            }
            group.registerIfAbsent(writer);

            SegmentWriteResult result = writeToSegment(idBytes, psi, group, writer);

            manifest.add(idKey, result.writer().getSegmentName(), result.offset(), phaseCenter);

            if (!metadata.isEmpty()) {
                if (batchCommitSize <= 1) {
                    metaStore.put(idKey, metadata);
                } else {
                    metaStore.putNoFlush(idKey, metadata);
                }
            }

            if (batchCommitSize <= 1) {
                manifest.flush();
                if (!metadata.isEmpty()) metaStore.flush();
            }

            group.updatePhaseStats(phaseCenter);
            if (batchCommitSize <= 1) {
                rebuildShardSelector();
            }

            if (deltaIndex != null) {
                deltaIndex.add(idKey, result.writer().getSegmentName());
                if (deltaIndex.exceedsThreshold(ivfDeltaMaxSize)) {
                    scheduleIndexRebuild();
                }
            }

            return idKey;

        } catch (SegmentOverflowException |
                 DuplicatePatternException |
                 InvalidWavePatternException e) {
            throw e;
        } catch (Exception e) {
            try {
                if (manifest.contains(idKey)) {
                    manifest.remove(idKey);
                }
            } catch (Throwable ignored2) {
            }
            try {
                if (metaStore.contains(idKey)) {
                    metaStore.remove(idKey);
                }
            } catch (Throwable ignored2) {
            }
            throw new RuntimeException("Insert failed: " + idKey, e);
        }
    }

    @Override
    public void delete(String idKey) throws PatternNotFoundException {
        ensureOpen();
        Objects.requireNonNull(idKey, "idKey must not be null");
        HashingUtil.parseAndValidateMd5(idKey);

        if (deltaBuffer != null) {
            long lsn = wal.nextLsn();
            WalRecord walRecord = WalRecord.delete(lsn, idKey);
            CompletableFuture<Long> durFuture = wal.append(walRecord);

            try (AutoLock ignored = AutoLock.write(globalLock)) {
                DeltaBuffer.Entry deltaEntry = deltaBuffer.remove(idKey, lsn);
                if (deltaEntry == null && !manifest.contains(idKey)) {
                    throw new PatternNotFoundException(idKey);
                }

                if (deltaEntry == null) {
                    ManifestIndex.PatternLocation loc = manifest.get(idKey);
                    SegmentWriter writer = getOrCreateWriter(loc.segmentName());
                    writer.markDeleted(loc.offset());
                    long newVersion = writer.flush();
                    writer.sync();
                    readerCache.updateVersion(writer.getSegmentName(), newVersion);

                    manifest.remove(idKey);
                    metaStore.remove(idKey);
                    manifest.flush();
                    metaStore.flush();

                    if (deltaIndex != null) {
                        deltaIndex.markDeleted(idKey);
                        if (ivfSource.currentIndex() != null) {
                            ivfSource.currentIndex().removePattern(idKey);
                        }
                    }
                    rebuildShardSelector();
                }
            }

            switch (walDurability) {
                case STRICT -> durFuture.join();
                case GROUP -> {
                    pendingWalFutures.add(durFuture);
                    if (pendingWalFutures.size() >= WAL_SYNC_THRESHOLD) {
                        flushWalFutures();
                    }
                }
                case ASYNC -> {}
            }
            return;
        }

        try (AutoLock ignored = AutoLock.write(globalLock)) {
            ManifestIndex.PatternLocation loc = manifest.get(idKey);
            if (loc == null) {
                throw new PatternNotFoundException(idKey);
            }

            SegmentWriter writer = getOrCreateWriter(loc.segmentName());
            writer.markDeleted(loc.offset());
            long newVersion = writer.flush();
            writer.sync();
            readerCache.updateVersion(writer.getSegmentName(), newVersion);

            manifest.remove(idKey);
            metaStore.remove(idKey);
            manifest.flush();
            metaStore.flush();

            if (deltaIndex != null) {
                deltaIndex.markDeleted(idKey);
                if (ivfSource.currentIndex() != null) {
                    ivfSource.currentIndex().removePattern(idKey);
                }
            }

            rebuildShardSelector();
        }
    }

    @Override
    public String replace(String oldId, WavePattern newPattern, Map<String, String> newMetadata)
            throws PatternNotFoundException, InvalidWavePatternException, DuplicatePatternException {

        ensureOpen();
        validateWavePatternLen(newPattern);
        Map<String, String> safeMetadata = newMetadata == null ? Map.of() : newMetadata;

        HashingUtil.parseAndValidateMd5(oldId);
        String newId = HashingUtil.computeContentHash(newPattern);
        byte[] newIdBytes = HashingUtil.parseAndValidateMd5(newId);

        if (deltaBuffer != null) {
            return replaceViaDelta(oldId, newId, newIdBytes, newPattern, safeMetadata);
        }

        return replaceLegacy(oldId, newId, newIdBytes, newPattern, safeMetadata);
    }

    private String replaceViaDelta(String oldId, String newId, byte[] newIdBytes,
                                    WavePattern newPattern, Map<String, String> metadata) {
        double phaseCenter = meanPhase(newPattern.phase());

        long lsn = wal.nextLsn();
        WalRecord walRecord = WalRecord.replace(lsn, oldId, newId, metadata, newPattern);
        CompletableFuture<Long> durFuture = wal.append(walRecord);

        try (AutoLock ignored = AutoLock.write(globalLock)) {
            boolean oldInDelta = deltaBuffer.contains(oldId);
            boolean oldInManifest = manifest.contains(oldId);
            if (!oldInDelta && !oldInManifest) {
                throw new PatternNotFoundException(oldId);
            }

            if (!oldId.equals(newId)) {
                if (manifest.contains(newId) || deltaBuffer.contains(newId)) {
                    throw new DuplicatePatternException("Replacement would collide: " + newId);
                }
            }

            deltaBuffer.remove(oldId, lsn);

            if (oldInManifest) {
                ManifestIndex.PatternLocation oldLoc = manifest.get(oldId);
                if (oldLoc != null) {
                    SegmentWriter oldWriter = getOrCreateWriter(oldLoc.segmentName());
                    oldWriter.markDeleted(oldLoc.offset());
                    long oldVersion = oldWriter.flush();
                    oldWriter.sync();
                    readerCache.updateVersion(oldWriter.getSegmentName(), oldVersion);
                    manifest.remove(oldId);
                    manifest.flush();
                }
                if (metaStore.contains(oldId)) {
                    metaStore.remove(oldId);
                }
                if (deltaIndex != null) {
                    deltaIndex.markDeleted(oldId);
                    if (ivfSource != null && ivfSource.currentIndex() != null) {
                        ivfSource.currentIndex().removePattern(oldId);
                    }
                }
                rebuildShardSelector();
            }

            deltaBuffer.add(newId, newPattern, metadata, phaseCenter, newIdBytes, lsn);
        }

        switch (walDurability) {
            case STRICT -> durFuture.join();
            case GROUP -> {
                pendingWalFutures.add(durFuture);
                if (pendingWalFutures.size() >= WAL_SYNC_THRESHOLD) {
                    flushWalFutures();
                }
            }
            case ASYNC -> {}
        }

        if (deltaBuffer.activeSize() >= sealThreshold) {
            scheduleSeal();
        }

        return newId;
    }

    private String replaceLegacy(String oldId, String newId, byte[] newIdBytes,
                                  WavePattern newPattern, Map<String, String> safeMetadata) {
        try (AutoLock ignored = AutoLock.write(globalLock)) {
            ManifestIndex.PatternLocation oldLoc = manifest.get(oldId);
            if (oldLoc == null) {
                throw new PatternNotFoundException(oldId);
            }

            if (!oldId.equals(newId) && manifest.contains(newId)) {
                throw new DuplicatePatternException("Replacement would collide: " + newId);
            }

            double phaseCenter = meanPhase(newPattern.phase());
            int bucket = computePhaseBucket(phaseCenter);
            String base = "phase-" + bucket;
            PhaseSegmentGroup group = getOrCreateGroup(base);

            SegmentWriter replaceWriter = group.getWritable();
            if (replaceWriter == null || replaceWriter.willOverflow(newPattern)) {
                replaceWriter = group.createAndRegisterNewSegment();
            }
            group.registerIfAbsent(replaceWriter);

            SegmentWriteResult result = writeToSegment(newIdBytes, newPattern, group, replaceWriter);

            try {
                SegmentWriter oldWriter = null;
                long oldVersion = -1L;

                if (!oldLoc.segmentName().equals(result.writer().getSegmentName())
                        || oldLoc.offset() != result.offset()) {
                    oldWriter = getOrCreateWriter(oldLoc.segmentName());
                    oldWriter.markDeleted(oldLoc.offset());
                    oldVersion = oldWriter.flush();
                    oldWriter.sync();
                }

                if (oldId.equals(newId)) {
                    manifest.replace(
                            oldId,
                            oldLoc.segmentName(),
                            oldLoc.offset(),
                            result.writer().getSegmentName(),
                            result.offset(),
                            phaseCenter
                    );
                } else {
                    manifest.replace(
                            oldId,
                            newId,
                            result.writer().getSegmentName(),
                            result.offset(),
                            phaseCenter
                    );
                }

                if (!oldId.equals(newId) && metaStore.contains(oldId)) {
                    metaStore.remove(oldId);
                }

                if (!safeMetadata.isEmpty()) {
                    metaStore.put(newId, safeMetadata);
                }

                manifest.flush();
                metaStore.flush();

                group.updatePhaseStats(phaseCenter);
                readerCache.updateVersion(result.writer().getSegmentName(), result.version());

                if (oldWriter != null) {
                    readerCache.updateVersion(oldWriter.getSegmentName(), oldVersion);
                }

                rebuildShardSelector();

                if (deltaIndex != null) {
                    deltaIndex.markDeleted(oldId);
                    if (ivfSource.currentIndex() != null) {
                        ivfSource.currentIndex().removePattern(oldId);
                    }
                    deltaIndex.add(newId, result.writer().getSegmentName());
                    if (deltaIndex.exceedsThreshold(ivfDeltaMaxSize)) {
                        scheduleIndexRebuild();
                    }
                }

                return newId;

            } catch (Exception rollbackEx) {
                try {
                    SegmentWriter writer = result.writer();
                    writer.markDeleted(result.offset());
                    long rollbackVersion = writer.flush();
                    writer.sync();
                    readerCache.updateVersion(writer.getSegmentName(), rollbackVersion);
                } catch (Exception i) {
                    System.err.println("Failed to rollback written pattern " + newId);
                }
                throw new RuntimeException("Replace failed after write: " + newId, rollbackEx);
            }
        }
    }

    @Override
    public List<ResonanceMatch> query(WavePattern query, int topK) {
        ensureOpen();
        validateWavePatternLen(query);
        if (topK <= 0) {
            return List.of();
        }

        if (!pendingWalFutures.isEmpty()) flushWalFutures();

        try (AutoLock ignored = AutoLock.read(globalLock)) {
            String queryId = HashingUtil.computeContentHash(query);

            List<HeapItem> collected;

            if (ivfSource != null && ivfSource.currentIndex() != null) {
                int effectiveProbe = exactEquivalence ? Integer.MAX_VALUE : ivfNProbe;
                CandidateSource.ScoredCandidates scored =
                        ivfSource.scoredCandidates(query, topK, effectiveProbe);
                if (!scored.exhaustive() && !scored.candidates().isEmpty()) {
                    collected = exactScoreFinalists(scored, query, queryId, topK);
                } else {
                    collected = collectMatchesFromCandidates(
                            ivfSource.candidates(query, topK), query, queryId, topK);
                }
            } else {
                List<SegmentWriter> writers = selectWritersForQuery(query);
                int threshold = Math.max(4, writers.size() / Math.max(1, tune.poolParallelism));

                collected = queryPool.invoke(
                        new MatchQueryTask(writers, query, queryId, topK, 0, writers.size(), threshold)
                );

                if (collected.size() < topK) {
                    Set<String> seen = new HashSet<>();
                    for (SegmentWriter writer : writers) {
                        seen.add(writer.getSegmentName());
                    }

                    List<SegmentWriter> rest = getAllWritersStream()
                            .filter(w -> !seen.contains(w.getSegmentName()))
                            .toList();

                    if (!rest.isEmpty()) {
                        List<HeapItem> extra = queryPool.invoke(
                                new MatchQueryTask(rest, query, queryId, topK, 0, rest.size(), threshold)
                        );
                        collected.addAll(extra);
                    }
                }
            }

            if (deltaBuffer != null && !deltaBuffer.isEmpty()) {
                for (DeltaBuffer.ScoredMatch sm : deltaBuffer.scoreDelta(query, queryId, resonanceKernel, topK)) {
                    collected.add(new HeapItem(sm.match(), sm.idMatch(), sm.exactMatch()));
                }
            }

            List<ResonanceMatch> prelim = deduplicateTopK(collected, h -> h.match().id(), RESULT_ORDER, topK)
                    .stream()
                    .map(HeapItem::match)
                    .toList();

            return materializePatterns(prelim);
        }
    }

    @Override
    public List<ResonanceMatchDetailed> queryDetailed(WavePattern query, int topK) {
        ensureOpen();
        validateWavePatternLen(query);
        if (topK <= 0) {
            return List.of();
        }

        if (!pendingWalFutures.isEmpty()) flushWalFutures();

        try (AutoLock ignored = AutoLock.read(globalLock)) {
            String queryId = HashingUtil.computeContentHash(query);

            List<HeapItemDetailed> collected;

            if (ivfSource != null && ivfSource.currentIndex() != null) {
                int effectiveProbe = exactEquivalence ? Integer.MAX_VALUE : ivfNProbe;
                CandidateSource.ScoredCandidates scored =
                        ivfSource.scoredCandidates(query, topK, effectiveProbe);
                if (!scored.exhaustive() && !scored.candidates().isEmpty()) {
                    collected = exactScoreFinalistsDetailed(scored, query, queryId, topK);
                } else {
                    System.err.println("WARN queryDetailed: sidecar unavailable, falling back to " +
                            "collectDetailedFromCandidates (slow path)");
                    collected = collectDetailedFromCandidates(
                            ivfSource.candidates(query, topK), query, queryId, topK);
                }
            } else {
                List<SegmentWriter> writers = selectWritersForQuery(query);
                int threshold = Math.max(4, writers.size() / Math.max(1, tune.poolParallelism));

                collected = queryPool.invoke(
                        new DetailedMatchQueryTask(writers, query, queryId, topK, 0, writers.size(), threshold)
                );

                if (collected.size() < topK) {
                    Set<String> seen = new HashSet<>();
                    for (SegmentWriter writer : writers) {
                        seen.add(writer.getSegmentName());
                    }

                    List<SegmentWriter> rest = getAllWritersStream()
                            .filter(w -> !seen.contains(w.getSegmentName()))
                            .toList();

                    if (!rest.isEmpty()) {
                        List<HeapItemDetailed> extra = queryPool.invoke(
                                new DetailedMatchQueryTask(rest, query, queryId, topK, 0, rest.size(), threshold)
                        );
                        collected.addAll(extra);
                    }
                }
            }

            if (deltaBuffer != null && !deltaBuffer.isEmpty()) {
                for (DeltaBuffer.ScoredMatchDetailed smd :
                        deltaBuffer.scoreDeltaDetailed(query, queryId, resonanceKernel, topK)) {
                    collected.add(new HeapItemDetailed(smd.match(), smd.idMatch(), smd.exactMatch()));
                }
            }

            return deduplicateTopK(collected, h -> h.match().id(), RESULT_ORDER_DETAILED, topK)
                    .stream()
                    .map(HeapItemDetailed::match)
                    .toList();
        }
    }

    @Override
    public InterferenceMap queryInterference(WavePattern query, int topK) {
        ensureOpen();
        return new InterferenceMap(query, queryDetailed(query, topK));
    }

    @Override
    public List<InterferenceEntry> queryInterferenceMap(WavePattern query, int topK) {
        ensureOpen();
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
        ensureOpen();
        Objects.requireNonNull(patterns, "patterns must not be null");
        if (patterns.isEmpty()) {
            throw new InvalidWavePatternException("Empty pattern list in composite query");
        }
        patterns.forEach(this::validateWavePatternLen);

        WavePattern superposed = WavePatternUtils.superpose(patterns, weights);
        return query(superposed, topK);
    }

    @Override
    public List<ResonanceMatchDetailed> queryCompositeDetailed(List<WavePattern> patterns, List<Double> weights, int topK) {
        ensureOpen();
        Objects.requireNonNull(patterns, "patterns must not be null");
        if (patterns.isEmpty()) {
            throw new InvalidWavePatternException("Empty pattern list in composite query");
        }
        patterns.forEach(this::validateWavePatternLen);

        WavePattern superposed = WavePatternUtils.superpose(patterns, weights);
        return queryDetailed(superposed, topK);
    }

    @Override
    public float compare(WavePattern a, WavePattern b) {
        ensureOpen();
        validateWavePatternLen(a);
        validateWavePatternLen(b);
        return resonanceKernel.compare(a, b);
    }

    public void forceIndexRebuild() {
        if (deltaBuffer != null && !deltaBuffer.isEmpty()) {
            sealDelta();
        }
        try (AutoLock ignored = AutoLock.write(globalLock)) {
            if (!pendingWriters.isEmpty()) {
                flushPendingWriters();
            }
        }
        if (ivfSource == null) return;
        rebuildIvfIndex();
    }

    public PhaseShardSelector getShardSelector() {
        return shardSelectorRef.get();
    }

    public void compactPhase(String baseName) {
        PhaseSegmentGroup group = segmentGroups.get(baseName);
        if (group != null && group.maybeCompact()) {
            rebuildShardSelector();
        }
    }

    public boolean containsExactPattern(WavePattern pattern) {
        ensureOpen();
        validateWavePatternLen(pattern);
        String hash = HashingUtil.computeContentHash(pattern);
        if (manifest.contains(hash)) return true;
        return deltaBuffer != null && deltaBuffer.contains(hash);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        ScheduledFuture<?> pendingSeal = sealFuture.getAndSet(null);
        if (pendingSeal != null) {
            pendingSeal.cancel(false);
        }

        if (!pendingWalFutures.isEmpty()) flushWalFutures();

        if (deltaBuffer != null && !deltaBuffer.isEmpty()) {
            try {
                sealDelta();
            } catch (Exception e) {
                System.err.println("Failed to drain delta buffer on close: " + e.getMessage());
            }
        }

        if (wal != null) {
            try {
                wal.close();
            } catch (Exception e) {
                System.err.println("Failed to close WAL: " + e.getMessage());
            }
        }

        if (ivfSource != null) {
            ivfSource.requestShutdown();
        }

        ScheduledFuture<?> pendingRebuild = rebuildFuture.getAndSet(null);
        if (pendingRebuild != null) {
            pendingRebuild.cancel(false);
        }

        synchronized (rebuildMutex) {
        }

        try (AutoLock ignored = AutoLock.write(globalLock)) {
            compactionTask.cancel(false);

            if (!pendingWriters.isEmpty()) {
                flushPendingWriters();
                manifest.flush();
                metaStore.flush();
            }

            if (ivfSource != null && centroidsPath != null) {
                try {
                    ivfSource.persistIndex(centroidsPath);
                } catch (Exception e) {
                    System.err.println("Failed to persist IVF index: " + e.getMessage());
                }
            }

            readerCache.close();
            segmentGroups.values().forEach(group -> group.getAll().forEach(this::safeClose));
            manifest.flush();
            metaStore.flush();
        } finally {
            if (ownRuntime) {
                runtime.close();
            }
        }
    }

    private static Method resolveCompareManyFlat(Object kernel) {
        try {
            return kernel.getClass().getMethod(
                    "compareManyFlat",
                    double[].class, double[].class, double[].class, double[].class, int.class, int.class
            );
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("WavePatternStoreImpl is already closed");
        }
    }

    private void safeCompactSweep() {
        if (closed.get()) {
            return;
        }
        try {
            segmentGroups.keySet().forEach(this::compactPhase);
        } catch (Throwable t) {
            System.err.println("Compaction sweep failed: " + t.getMessage());
        }
    }

    private List<HeapItem> collectMatchesFromWriter(SegmentWriter writer,
                                                    WavePattern query,
                                                    String queryId,
                                                    int topK) {
        if (writer == null) {
            return List.of();
        }

        final Comparator<HeapItem> cmp = HEAP_ORDER;
        final CachedReader reader = readerCache.get(writer.getSegmentName());
        if (reader == null) {
            return List.of();
        }

        final int len = query.amplitude().length;
        final int batchSize = tune.batchSizeForLen(len, activeTasksEstimate());
        final int localCap = Math.max(topK, 8);
        final boolean useFlat = compareManyFlatMethod != null;

        final PriorityQueue<HeapItem> heap = new PriorityQueue<>(localCap, cmp);
        final FlatBuffers fb = TL_FLAT.get();
        fb.ensure(len, batchSize);

        int inBatch = 0;
        for (String id : reader.allIds()) {
            fb.ids[inBatch++] = id;
            if (inBatch == batchSize) {
                processMatchBatch(reader, query, queryId, topK, len, inBatch, useFlat, fb, heap, cmp);
                inBatch = 0;
            }
        }

        if (inBatch > 0) {
            processMatchBatch(reader, query, queryId, topK, len, inBatch, useFlat, fb, heap, cmp);
        }

        return new ArrayList<>(heap);
    }

    private void processMatchBatch(CachedReader reader,
                                   WavePattern query,
                                   String queryId,
                                   int topK,
                                   int len,
                                   int count,
                                   boolean useFlat,
                                   FlatBuffers fb,
                                   PriorityQueue<HeapItem> heap,
                                   Comparator<HeapItem> cmp) {
        acquireIoPermitBatch();
        try {
            if (useFlat) {
                int ready = fillFlatBatch(reader, fb, len, count);
                if (ready > 0) {
                    scoreAndMergeFlat(query, queryId, fb, len, ready, heap, cmp, topK);
                }
            } else {
                List<String> idBatch = new ArrayList<>(count);
                List<WavePattern> candBatch = new ArrayList<>(count);
                int ready = fillObjectBatch(reader, fb.ids, count, idBatch, candBatch, len);
                if (ready > 0) {
                    scoreAndMergeObject(query, queryId, idBatch, candBatch, heap, cmp, topK);
                }
            }
        } finally {
            releaseIoPermitBatch();
        }
    }

    private List<HeapItemDetailed> collectDetailedFromWriter(SegmentWriter writer,
                                                             WavePattern query,
                                                             String queryId,
                                                             int topK) {
        if (writer == null) {
            return List.of();
        }

        final Comparator<HeapItemDetailed> cmp = HEAP_ORDER_DETAILED;
        final CachedReader reader = readerCache.get(writer.getSegmentName());
        if (reader == null) {
            return List.of();
        }

        final int len = query.amplitude().length;

        final PriorityQueue<HeapItemDetailed> heap = new PriorityQueue<>(Math.max(topK, 8), cmp);

        acquireIoPermitBatch();
        try {
            for (String id : reader.allIds()) {
                WavePattern cand = readNoSemaphore(reader, id);
                if (cand == null || cand.amplitude().length != len) {
                    continue;
                }

                ComparisonResult result = resonanceKernel.compareWithPhaseDelta(query, cand);
                float energy = result.energy();
                double phaseShift = result.phaseDelta();
                ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
                double zoneScore = zone.score();

                boolean idEq = id.equals(queryId);
                boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
                HeapItemDetailed item = new HeapItemDetailed(
                        new ResonanceMatchDetailed(id, energy, cand, phaseShift, zone, zoneScore),
                        idEq, exactEq);
                heapAddOrEvict(heap, item, cmp, topK);
            }
            return new ArrayList<>(heap);
        } finally {
            releaseIoPermitBatch();
        }
    }

    private int fillFlatBatch(CachedReader reader, FlatBuffers fb, int len, int count) {
        int ready = 0;
        for (int i = 0; i < count; i++) {
            String id = fb.ids[i];
            boolean ok = reader.readPatternFlat(id,
                    fb.ampFlat, ready * len,
                    fb.phaseFlat, ready * len,
                    len);
            if (!ok) continue;
            fb.ids[ready] = id;
            ready++;
        }
        return ready;
    }

    private int fillObjectBatch(CachedReader reader,
                                String[] idsSrc,
                                int count,
                                List<String> idsOut,
                                List<WavePattern> candsOut,
                                int len) {
        int ready = 0;
        for (int i = 0; i < count; i++) {
            String id = idsSrc[i];
            WavePattern cand = readNoSemaphore(reader, id);
            if (cand == null || cand.amplitude().length != len) {
                continue;
            }
            idsOut.add(id);
            candsOut.add(cand);
            ready++;
        }
        return ready;
    }

    private void scoreAndMergeObject(WavePattern query,
                                     String queryId,
                                     List<String> ids,
                                     List<WavePattern> cands,
                                     PriorityQueue<HeapItem> heap,
                                     Comparator<HeapItem> cmp,
                                     int topK) {
        float[] scores = resonanceKernel.compareMany(query, cands);

        for (int i = 0; i < scores.length; i++) {
            String id = ids.get(i);
            WavePattern cand = cands.get(i);
            float energy = scores[i];

            boolean idEq = id.equals(queryId);
            boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
            tracer.trace(id, query, cand, energy);
            HeapItem item = new HeapItem(new ResonanceMatch(id, energy, cand), idEq, exactEq);
            heapAddOrEvict(heap, item, cmp, topK);
        }
    }

    private void scoreAndMergeFlat(WavePattern query,
                                   String queryId,
                                   FlatBuffers fb,
                                   int len,
                                   int count,
                                   PriorityQueue<HeapItem> heap,
                                   Comparator<HeapItem> cmp,
                                   int topK) {

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
                double[] a = Arrays.copyOfRange(fb.ampFlat, i * len, (i + 1) * len);
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
            HeapItem item = new HeapItem(new ResonanceMatch(id, energy, null), idEq, exactEq);
            heapAddOrEvict(heap, item, cmp, topK);
        }
    }

    private List<HeapItem> exactScoreFinalists(CandidateSource.ScoredCandidates scored,
                                                WavePattern query, String queryId, int topK) {
        int overfetch = tune.overfetchForTopK(topK);

        List<PostingSidecar.ScoredCandidate> ivfCandidates = new ArrayList<>();
        List<PostingSidecar.ScoredCandidate> deltaCandidates = new ArrayList<>();
        for (PostingSidecar.ScoredCandidate sc : scored.candidates()) {
            if (sc.exactOnly()) {
                deltaCandidates.add(sc);
            } else {
                ivfCandidates.add(sc);
            }
        }

        int finalistCount = Math.min(topK * overfetch, ivfCandidates.size());
        ivfCandidates.sort(Comparator.comparingDouble((PostingSidecar.ScoredCandidate sc) -> -sc.approxScore())
                .thenComparing(PostingSidecar.ScoredCandidate::id));

        Comparator<HeapItem> cmp = HEAP_ORDER;
        PriorityQueue<HeapItem> heap = new PriorityQueue<>(Math.max(topK, 8), cmp);

        int len = query.amplitude().length;
        FlatBuffers fb = TL_FLAT.get();
        fb.ensure(len, 1);

        for (int i = 0; i < finalistCount; i++) {
            exactScoreCandidate(ivfCandidates.get(i).id(), query, queryId, len, fb, heap, cmp, topK);
        }

        for (PostingSidecar.ScoredCandidate dc : deltaCandidates) {
            exactScoreCandidate(dc.id(), query, queryId, len, fb, heap, cmp, topK);
        }

        return new ArrayList<>(heap);
    }

    private void exactScoreCandidate(String id, WavePattern query, String queryId,
                                      int len, FlatBuffers fb,
                                      PriorityQueue<HeapItem> heap, Comparator<HeapItem> cmp,
                                      int topK) {
        ManifestIndex.PatternLocation loc = manifest.get(id);
        if (loc == null) return;

        CachedReader reader = readerCache.getOrLoad(loc.segmentName());
        if (reader == null) return;

        if (!reader.readPatternFlat(id, fb.ampFlat, 0, fb.phaseFlat, 0, len)) return;

        WavePattern cand = new WavePattern(
                java.util.Arrays.copyOf(fb.ampFlat, len),
                java.util.Arrays.copyOf(fb.phaseFlat, len));

        float energy = resonanceKernel.compare(query, cand);

        boolean idEq = id.equals(queryId);
        boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;
        HeapItem item = new HeapItem(new ResonanceMatch(id, energy, cand), idEq, exactEq);
        heapAddOrEvict(heap, item, cmp, topK);
    }

    private List<HeapItemDetailed> exactScoreFinalistsDetailed(
            CandidateSource.ScoredCandidates scored,
            WavePattern query, String queryId, int topK) {

        int overfetch = tune.overfetchForTopK(topK);

        List<PostingSidecar.ScoredCandidate> ivfCandidates = new ArrayList<>();
        List<PostingSidecar.ScoredCandidate> deltaCandidates = new ArrayList<>();
        for (PostingSidecar.ScoredCandidate sc : scored.candidates()) {
            if (sc.exactOnly()) {
                deltaCandidates.add(sc);
            } else {
                ivfCandidates.add(sc);
            }
        }

        int finalistCount = Math.min(topK * overfetch, ivfCandidates.size());
        ivfCandidates.sort(Comparator.comparingDouble((PostingSidecar.ScoredCandidate sc) -> -sc.approxScore())
                .thenComparing(PostingSidecar.ScoredCandidate::id));

        Comparator<HeapItemDetailed> cmp = HEAP_ORDER_DETAILED;
        int localCap = Math.max(topK, 8);
        PriorityQueue<HeapItemDetailed> heap = new PriorityQueue<>(localCap, cmp);

        int len = query.amplitude().length;
        FlatBuffers fb = TL_FLAT.get();
        fb.ensure(len, 1);

        for (int i = 0; i < finalistCount; i++) {
            exactScoreCandidateDetailed(ivfCandidates.get(i).id(), query, queryId, len, fb, heap, cmp, topK);
        }

        for (PostingSidecar.ScoredCandidate dc : deltaCandidates) {
            exactScoreCandidateDetailed(dc.id(), query, queryId, len, fb, heap, cmp, topK);
        }

        return new ArrayList<>(heap);
    }

    private void exactScoreCandidateDetailed(String id, WavePattern query, String queryId,
                                              int len, FlatBuffers fb,
                                              PriorityQueue<HeapItemDetailed> heap,
                                              Comparator<HeapItemDetailed> cmp, int topK) {
        ManifestIndex.PatternLocation loc = manifest.get(id);
        if (loc == null) return;

        CachedReader reader = readerCache.getOrLoad(loc.segmentName());
        if (reader == null) return;

        if (!reader.readPatternFlat(id, fb.ampFlat, 0, fb.phaseFlat, 0, len)) return;

        WavePattern cand = new WavePattern(
                java.util.Arrays.copyOf(fb.ampFlat, len),
                java.util.Arrays.copyOf(fb.phaseFlat, len));

        ComparisonResult result = resonanceKernel.compareWithPhaseDelta(query, cand);
        float energy = result.energy();
        double phaseShift = result.phaseDelta();
        ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
        double zoneScore = zone.score();

        boolean idEq = id.equals(queryId);
        boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;

        HeapItemDetailed item = new HeapItemDetailed(
                new ResonanceMatchDetailed(id, energy, cand, phaseShift, zone, zoneScore),
                idEq, exactEq);
        heapAddOrEvict(heap, item, cmp, topK);
    }

    private List<HeapItem> collectMatchesFromCandidates(
            Map<String, Collection<String>> candidatesBySegment,
            WavePattern query, String queryId, int topK) {

        if (candidatesBySegment.size() <= 1) {
            return collectMatchesFromCandidatesSequential(candidatesBySegment, query, queryId, topK);
        }

        List<ForkJoinTask<List<HeapItem>>> tasks = new ArrayList<>(candidatesBySegment.size());
        for (var entry : candidatesBySegment.entrySet()) {
            String segName = entry.getKey();
            Collection<String> ids = entry.getValue();
            tasks.add(queryPool.submit(() ->
                    collectMatchesFromCandidatesSequential(
                            Map.of(segName, ids), query, queryId, topK)));
        }

        List<HeapItem> collected = new ArrayList<>();
        for (var task : tasks) {
            try {
                collected.addAll(task.join());
            } catch (Exception e) {
                System.err.println("WARN: candidate scoring failed for segment: " + e.getMessage());
            }
        }
        return collected;
    }

    private List<HeapItem> collectMatchesFromCandidatesSequential(
            Map<String, Collection<String>> candidatesBySegment,
            WavePattern query, String queryId, int topK) {

        final Comparator<HeapItem> cmp = HEAP_ORDER;
        final int len = query.amplitude().length;
        final int batchSize = tune.batchSizeForLen(len, activeTasksEstimate());
        final boolean useFlat = compareManyFlatMethod != null;
        final int localCap = Math.max(topK, 8);

        final PriorityQueue<HeapItem> heap = new PriorityQueue<>(localCap, cmp);
        final FlatBuffers fb = TL_FLAT.get();
        fb.ensure(len, batchSize);

        for (var entry : candidatesBySegment.entrySet()) {
            CachedReader reader = readerCache.getOrLoad(entry.getKey());
            if (reader == null) continue;

            int inBatch = 0;
            for (String id : entry.getValue()) {
                fb.ids[inBatch++] = id;
                if (inBatch == batchSize) {
                    processMatchBatch(reader, query, queryId, topK, len,
                            inBatch, useFlat, fb, heap, cmp);
                    inBatch = 0;
                }
            }
            if (inBatch > 0) {
                processMatchBatch(reader, query, queryId, topK, len,
                        inBatch, useFlat, fb, heap, cmp);
            }
        }

        return new ArrayList<>(heap);
    }

    private List<HeapItemDetailed> collectDetailedFromCandidates(
            Map<String, Collection<String>> candidatesBySegment,
            WavePattern query, String queryId, int topK) {

        if (candidatesBySegment.size() <= 1) {
            return collectDetailedFromCandidatesSequential(candidatesBySegment, query, queryId, topK);
        }

        List<ForkJoinTask<List<HeapItemDetailed>>> tasks = new ArrayList<>(candidatesBySegment.size());
        for (var entry : candidatesBySegment.entrySet()) {
            String segName = entry.getKey();
            Collection<String> ids = entry.getValue();
            tasks.add(queryPool.submit(() ->
                    collectDetailedFromCandidatesSequential(
                            Map.of(segName, ids), query, queryId, topK)));
        }

        List<HeapItemDetailed> collected = new ArrayList<>();
        for (var task : tasks) {
            try {
                collected.addAll(task.join());
            } catch (Exception e) {
                System.err.println("WARN: detailed candidate scoring failed for segment: " + e.getMessage());
            }
        }
        return collected;
    }

    private List<HeapItemDetailed> collectDetailedFromCandidatesSequential(
            Map<String, Collection<String>> candidatesBySegment,
            WavePattern query, String queryId, int topK) {

        final Comparator<HeapItemDetailed> cmp = HEAP_ORDER_DETAILED;
        final int len = query.amplitude().length;

        final PriorityQueue<HeapItemDetailed> heap = new PriorityQueue<>(Math.max(topK, 8), cmp);

        for (var entry : candidatesBySegment.entrySet()) {
            CachedReader reader = readerCache.getOrLoad(entry.getKey());
            if (reader == null) continue;

            for (String id : entry.getValue()) {
                WavePattern cand = readNoSemaphore(reader, id);
                if (cand == null || cand.amplitude().length != len) continue;

                ComparisonResult result = resonanceKernel.compareWithPhaseDelta(query, cand);
                float energy = result.energy();
                double phaseShift = result.phaseDelta();
                ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
                double zoneScore = zone.score();

                boolean idEq = id.equals(queryId);
                boolean exactEq = energy > 1.0f - EXACT_MATCH_EPS;

                HeapItemDetailed item = new HeapItemDetailed(
                        new ResonanceMatchDetailed(id, energy, cand, phaseShift, zone, zoneScore),
                        idEq, exactEq);
                heapAddOrEvict(heap, item, cmp, topK);
            }
        }

        return new ArrayList<>(heap);
    }

    private void scheduleSeal() {
        if (closed.get()) return;
        if (!sealScheduled.compareAndSet(false, true)) return;

        ScheduledFuture<?> future = runtime.scheduler().schedule(() -> {
            try {
                if (!closed.get()) {
                    sealDelta();
                }
            } catch (Throwable t) {
                System.err.println("Delta seal failed: " + t.getMessage());
                t.printStackTrace(System.err);
            } finally {
                sealScheduled.set(false);
                sealFuture.set(null);
            }
        }, 100, TimeUnit.MILLISECONDS);
        sealFuture.set(future);
    }

    public void sealDelta() {
        if (deltaBuffer == null) return;

        if (!pendingWalFutures.isEmpty()) flushWalFutures();

        synchronized (sealMutex) {
            if (deltaBuffer.isEmpty()) return;

            Map<String, DeltaBuffer.Entry> frozenEntries;
            long frozenMaxLsn = 0;
            try (AutoLock ignored = AutoLock.write(globalLock)) {
                frozenEntries = deltaBuffer.freeze();
            }
            if (frozenEntries.isEmpty()) {
                deltaBuffer.clearFrozen();
                return;
            }

            Map<String, Long> tombstones = deltaBuffer.tombstones();
            frozenEntries.entrySet().removeIf(e -> {
                Long deleteLsn = tombstones.get(e.getKey());
                return deleteLsn != null && deleteLsn > e.getValue().lsn();
            });

            record PendingManifestEntry(String id, String segmentName, long offset,
                                        double phaseCenter, Map<String, String> metadata) {}
            List<PendingManifestEntry> pendingManifest = new ArrayList<>(frozenEntries.size());
            Set<SegmentWriter> touchedWriters = new LinkedHashSet<>();

            for (DeltaBuffer.Entry entry : frozenEntries.values()) {
                frozenMaxLsn = Math.max(frozenMaxLsn, entry.lsn());

                double phaseCenter = entry.phaseCenter();
                int bucket = computePhaseBucket(phaseCenter);
                String base = "phase-" + bucket;
                PhaseSegmentGroup group = getOrCreateGroup(base);

                SegmentWriter writer = group.getWritable();
                if (writer == null || writer.willOverflow(entry.pattern())) {
                    writer = group.createAndRegisterNewSegment();
                }
                group.registerIfAbsent(writer);

                try {
                    long offset = writer.write(entry.idBytes(), entry.pattern());
                    touchedWriters.add(writer);
                    pendingManifest.add(new PendingManifestEntry(
                            entry.id(), writer.getSegmentName(), offset,
                            phaseCenter, entry.metadata()));
                } catch (Exception e) {
                    System.err.println("Seal write failed for " + entry.id() + ": " + e.getMessage());
                }
            }

            for (SegmentWriter w : touchedWriters) {
                try {
                    long ver = w.flush();
                    w.sync();
                    registerSegment(w);
                } catch (Exception e) {
                    System.err.println("Seal flush failed for " + w.getSegmentName() + ": " + e.getMessage());
                }
            }

            try (AutoLock ignored = AutoLock.write(globalLock)) {
                for (PendingManifestEntry pme : pendingManifest) {
                    if (!manifest.contains(pme.id)) {
                        manifest.add(pme.id, pme.segmentName, pme.offset, pme.phaseCenter);
                    }
                    if (pme.metadata != null && !pme.metadata.isEmpty()) {
                        metaStore.putNoFlush(pme.id, pme.metadata);
                    }
                    if (deltaIndex != null) {
                        deltaIndex.add(pme.id, pme.segmentName);
                    }
                }
                manifest.flush();
                metaStore.flush();
                deltaBuffer.clearFrozen();
                rebuildShardSelector();
            }

            if (wal != null && frozenMaxLsn > 0) {
                try {
                    wal.checkpoint(frozenMaxLsn);
                } catch (Exception e) {
                    System.err.println("WAL checkpoint failed: " + e.getMessage());
                }
            }

            if (deltaIndex != null && deltaIndex.exceedsThreshold(ivfDeltaMaxSize)) {
                scheduleIndexRebuild();
            }
        }
    }

    private void flushWalFutures() {
        if (pendingWalFutures.isEmpty()) return;
        List<CompletableFuture<Long>> snapshot;
        synchronized (pendingWalFutures) {
            snapshot = new ArrayList<>(pendingWalFutures);
            pendingWalFutures.clear();
        }
        CompletableFuture.allOf(snapshot.toArray(CompletableFuture[]::new)).join();
    }

    private void awaitSealCompletion() {
        scheduleSeal();

        long deadline = System.currentTimeMillis() + backpressureTimeoutMs;
        synchronized (sealMutex) {
        }
        if (deltaBuffer.size() >= deltaMaxEntries &&
                System.currentTimeMillis() < deadline) {
            sealDelta();
        }
    }

    private void replayWalRecord(WalRecord record) {
        switch (record.type()) {
            case WalRecord.TYPE_INSERT -> {
                WalRecord.InsertPayload p = record.decodeInsert();
                if (manifest.contains(p.contentHash())) return;
                if (deltaBuffer.contains(p.contentHash())) return;

                double phaseCenter = meanPhase(p.pattern().phase());
                byte[] idBytes = HashingUtil.parseAndValidateMd5(p.contentHash());
                deltaBuffer.add(p.contentHash(), p.pattern(), p.metadata(),
                        phaseCenter, idBytes, record.lsn());
            }
            case WalRecord.TYPE_DELETE -> {
                String deleteId = record.decodeDeleteId();
                deltaBuffer.markDeleted(deleteId, record.lsn());
            }
            case WalRecord.TYPE_REPLACE -> {
                WalRecord.ReplacePayload rp = record.decodeReplace();
                deltaBuffer.markDeleted(rp.oldId(), record.lsn());

                WalRecord.InsertPayload ip = rp.insert();
                if (!manifest.contains(ip.contentHash()) && !deltaBuffer.contains(ip.contentHash())) {
                    double phaseCenter = meanPhase(ip.pattern().phase());
                    byte[] idBytes = HashingUtil.parseAndValidateMd5(ip.contentHash());
                    deltaBuffer.add(ip.contentHash(), ip.pattern(), ip.metadata(),
                            phaseCenter, idBytes, record.lsn());
                }
            }
            case WalRecord.TYPE_CHECKPOINT -> {
            }
            default -> System.err.println("WARN WAL replay: unknown record type " + record.type());
        }
    }

    private void scheduleIndexRebuild() {
        if (closed.get()) return;
        if (!indexRebuildScheduled.compareAndSet(false, true)) return;

        ScheduledFuture<?> future = runtime.scheduler().schedule(() -> {
            try {
                if (!closed.get()) {
                    rebuildIvfIndex();
                }
            } catch (Throwable t) {
                System.err.println("IVF index rebuild failed: " + t.getMessage());
            } finally {
                indexRebuildScheduled.set(false);
                rebuildFuture.set(null);
            }
        }, 500, TimeUnit.MILLISECONDS);
        rebuildFuture.set(future);
    }

    private void rebuildIvfIndex() {
        if (ivfSource == null || closed.get()) return;

        synchronized (rebuildMutex) {
            if (closed.get()) return;

            Set<String> segNames;
            try (AutoLock ignored = AutoLock.read(globalLock)) {
                if (closed.get()) return;
                segNames = manifest.getAllSegmentNames();
            }

            Map<String, CachedReader> pinned = new ConcurrentHashMap<>();
            try {
                ivfSource.buildIndexWithVamana(
                        segName -> {
                            if (closed.get()) return null;
                            CachedReader r = pinned.get(segName);
                            if (r != null) return r;
                            r = readerCache.getOrLoad(segName);
                            if (r != null) {
                                try {
                                    r.acquire();
                                    pinned.put(segName, r);
                                } catch (IllegalStateException e) {
                                    return null;
                                }
                            }
                            return r;
                        },
                        segNames,
                        patternLen,
                        System.nanoTime()
                );

                if (centroidsPath != null) {
                    ivfSource.persistIndex(centroidsPath);
                }
            } catch (Exception e) {
                System.err.println("IVF index build/persist error: " + e.getMessage());
                e.printStackTrace(System.err);
            } finally {
                pinned.values().forEach(CachedReader::release);
                pinned.clear();
            }
        }
    }

    private void repopulatePostings() {
        if (ivfSource == null || ivfSource.currentIndex() == null) return;

        List<CachedReader> pinnedReaders = new ArrayList<>();
        try {
            for (String segName : manifest.getAllSegmentNames()) {
                CachedReader reader = readerCache.getOrLoad(segName);
                if (reader == null) continue;
                try {
                    reader.acquire();
                    pinnedReaders.add(reader);
                } catch (IllegalStateException e) {
                    continue;
                }
                for (String id : reader.allIds()) {
                    if (!manifest.contains(id)) continue;
                    WavePattern p = readNoSemaphore(reader, id);
                    if (p != null && p.amplitude().length == patternLen) {
                        ivfSource.currentIndex().assignPattern(id, p);
                    }
                }
            }
        } finally {
            pinnedReaders.forEach(CachedReader::release);
        }
    }

    private void rebuildSidecar() {
        if (ivfSource == null || ivfSource.currentIndex() == null) return;

        Set<String> segNames = manifest.getAllSegmentNames();
        Map<String, CachedReader> pinned = new HashMap<>();
        try {
            ivfSource.rebuildSidecar(
                    segName -> {
                        CachedReader r = pinned.get(segName);
                        if (r != null) return r;
                        r = readerCache.getOrLoad(segName);
                        if (r != null) {
                            try {
                                r.acquire();
                                pinned.put(segName, r);
                            } catch (IllegalStateException e) {
                                return null;
                            }
                        }
                        return r;
                    },
                    segNames,
                    patternLen
            );
        } finally {
            pinned.values().forEach(CachedReader::release);
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

        PhaseSegmentGroup group = segmentGroups.computeIfAbsent(
                groupKey,
                key -> new PhaseSegmentGroup(key, rootDir.resolve("segments"), this.compactor)
        );

        Optional<SegmentWriter> existing = group.getAll().stream()
                .filter(sw -> sw.getSegmentName().equals(segmentName))
                .findFirst();

        if (existing.isPresent()) {
            return existing.get();
        }

        SegmentWriter writer = new SegmentWriter(rootDir.resolve("segments").resolve(segmentName));
        registerSegment(writer);
        return writer;
    }

    private PhaseShardSelector createShardSelector() {
        List<ManifestIndex.PatternLocation> locations = manifest.getAllLocations();
        return locations.isEmpty()
                ? PhaseShardSelector.emptyFallback()
                : PhaseShardSelector.fromManifest(locations, READ_EPSILON);
    }

    private void rebuildShardSelector() {
        shardSelectorRef.set(createShardSelector());
    }

    private String base(String segmentName) {
        int dot = segmentName.indexOf(".segment");
        String raw = dot >= 0 ? segmentName.substring(0, dot) : segmentName;

        int lastDash = raw.lastIndexOf('-');
        if (lastDash > 0) {
            return raw.substring(0, lastDash);
        }
        return raw;
    }

    private PhaseSegmentGroup getOrCreateGroup(String baseName) {
        return segmentGroups.computeIfAbsent(
                baseName,
                name -> new PhaseSegmentGroup(name, rootDir.resolve("segments"), compactor)
        );
    }

    private void registerSegment(SegmentWriter writer) {
        manifest.registerSegmentIfAbsent(writer.getSegmentName());
        getOrCreateGroup(base(writer.getSegmentName())).registerIfAbsent(writer);
        readerCache.updateVersion(writer.getSegmentName(), writer.getWriteOffset());
    }

    private void loadAllWritersFromManifest() {
        for (String seg : manifest.getAllSegmentNames()) {
            PhaseSegmentGroup group = getOrCreateGroup(base(seg));
            SegmentWriter writer = group.getAll().stream()
                    .filter(sw -> sw.getSegmentName().equals(seg))
                    .findFirst()
                    .orElseGet(() -> new SegmentWriter(rootDir.resolve("segments").resolve(seg)));
            registerSegment(writer);
        }
    }

    private Stream<SegmentWriter> getAllWritersStream() {
        return segmentGroups.values().stream()
                .flatMap(group -> group.getAll().stream())
                .distinct();
    }

    private static double meanPhase(double[] phase) {
        double sum = 0.0;
        for (double v : phase) {
            sum += v;
        }
        return sum / phase.length;
    }

    private int computePhaseBucket(double phaseCenter) {
        return normBucketIndex((int) Math.floor((phaseCenter + Math.PI) / BUCKET_WIDTH_RAD));
    }

    private int normBucketIndex(int idx) {
        int maxIdx = maxBucketIndex();
        if (idx < 0) {
            return 0;
        }
        if (idx > maxIdx) {
            return maxIdx;
        }
        return idx;
    }

    private int maxBucketIndex() {
        return (int) Math.ceil((2 * Math.PI) / BUCKET_WIDTH_RAD) - 1;
    }

    private SegmentWriteResult writeToSegment(byte[] idBytes, WavePattern psi,
                                              PhaseSegmentGroup group,
                                              SegmentWriter writer) {
        while (true) {
            try {
                long offset = writer.write(idBytes, psi);

                if (batchCommitSize <= 1) {
                    long version = writer.flush();
                    writer.sync();
                    registerSegment(writer);
                    return new SegmentWriteResult(writer, offset, version);
                } else {
                    pendingWriters.add(writer);
                    pendingFlushCount++;

                    if (pendingFlushCount >= batchCommitSize) {
                        long version = flushPendingWriters();
                        return new SegmentWriteResult(writer, offset, version);
                    }

                    manifest.registerSegmentIfAbsent(writer.getSegmentName());
                    return new SegmentWriteResult(writer, offset, writer.getWriteOffset());
                }
            } catch (SegmentOverflowException ignored) {
                writer = group.createAndRegisterNewSegment();
                group.registerIfAbsent(writer);
            }
        }
    }

    private long flushPendingWriters() {
        long latestVersion = 0;
        for (SegmentWriter w : pendingWriters) {
            try {
                long ver = w.flush();
                w.sync();
                registerSegment(w);
                latestVersion = Math.max(latestVersion, ver);
            } catch (Exception e) {
                System.err.println("Batch flush failed for " + w.getSegmentName() + ": " + e.getMessage());
            }
        }
        pendingWriters.clear();
        pendingFlushCount = 0;
        manifest.flush();
        metaStore.flush();
        rebuildShardSelector();
        return latestVersion;
    }

    private <T> List<T> deduplicateTopK(List<T> items,
                                        Function<T, String> idExtractor,
                                        Comparator<? super T> order,
                                        int topK) {
        Map<String, T> best = new HashMap<>();
        for (T item : items) {
            String id = idExtractor.apply(item);
            best.merge(id, item, (a, b) -> order.compare(a, b) >= 0 ? a : b);
        }
        return best.values().stream()
                .sorted(order)
                .limit(topK)
                .toList();
    }

    private void acquireIoPermitBatch() {
        runtime.ioGovernor().acquire();
    }

    private void releaseIoPermitBatch() {
        runtime.ioGovernor().release();
    }

    private WavePattern readNoSemaphore(CachedReader reader, String id) {
        try {
            return reader.readById(id);
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private List<ResonanceMatch> materializePatterns(List<ResonanceMatch> matches) {
        boolean needMaterialization = false;
        for (ResonanceMatch match : matches) {
            if (match.pattern() == null) {
                needMaterialization = true;
                break;
            }
        }
        if (!needMaterialization) {
            return matches;
        }

        List<ResonanceMatch> out = new ArrayList<>(matches.size());
        for (ResonanceMatch match : matches) {
            if (match.pattern() != null) {
                out.add(match);
                continue;
            }

            String id = match.id();
            WavePattern pattern = null;
            ManifestIndex.PatternLocation loc = manifest.get(id);
            if (loc != null) {
                CachedReader reader = readerCache.get(loc.segmentName());
                if (reader != null) {
                    pattern = readNoSemaphore(reader, id);
                }
            }

            out.add(new ResonanceMatch(id, match.energy(), pattern));
        }
        return out;
    }

    private int activeTasksEstimate() {
        int active = queryPool.getActiveThreadCount();
        if (active <= 0) {
            active = Math.max(1, queryPool.getPoolSize());
        }
        return active;
    }

    private void validateWavePatternLen(WavePattern pattern) {
        Objects.requireNonNull(pattern, "pattern must not be null");
        Objects.requireNonNull(pattern.amplitude(), "pattern amplitude must not be null");
        Objects.requireNonNull(pattern.phase(), "pattern phase must not be null");

        int ampLen = pattern.amplitude().length;
        int phaseLen = pattern.phase().length;

        if (ampLen != phaseLen) {
            throw new InvalidWavePatternException(
                    "Amplitude / phase length mismatch: amp=" + ampLen + ", phase=" + phaseLen
            );
        }
        if (ampLen != patternLen) {
            throw new InvalidWavePatternException(
                    "Invalid pattern length: expected=" + patternLen + ", got=" + ampLen
            );
        }
    }

    private void verifyPatternLengthOnOpen() {
        Set<String> segments = manifest.getAllSegmentNames();
        if (segments.isEmpty()) {
            return;
        }

        for (String seg : segments) {
            CachedReader reader = readerCache.get(seg);
            if (reader == null) {
                continue;
            }

            OptionalInt sample = samplePatternLength(reader);
            if (sample.isEmpty()) {
                continue;
            }

            int len = sample.getAsInt();
            if (len != patternLen) {
                throw new InvalidWavePatternException(
                        "DB pattern length mismatch on open: expected=" + patternLen +
                                ", got=" + len + " in segment=" + seg +
                                ". Set -Dresonance.pattern.len=" + len + " to open this DB, or rebuild the DB."
                );
            }
        }
    }

    private OptionalInt samplePatternLength(CachedReader reader) {
        try {
            return reader.lazyStream()
                    .mapToInt(p -> p.pattern().amplitude().length)
                    .findFirst();
        } catch (RuntimeException e) {
            return OptionalInt.empty();
        }
    }

    private void safeClose(SegmentWriter writer) {
        try {
            writer.close();
        } catch (Exception e) {
            System.err.println("Failed to close writer: " + writer.getSegmentName());
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
            this.threshold = Math.max(2, threshold);
        }

        protected abstract List<T> process(SegmentWriter writer);

        @Override
        protected List<T> compute() {
            int span = to - from;
            if (span <= threshold) {
                List<T> results = new ArrayList<>(Math.max(span * 2, 4));
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

                if (!rightResult.isEmpty()) {
                    leftResult.addAll(rightResult);
                }
                return leftResult;
            }
        }

        protected abstract QueryTask<T> cloneFor(int from, int to, int threshold);
    }

    private final class MatchQueryTask extends QueryTask<HeapItem> {
        private final WavePattern query;
        private final String queryId;
        private final int topK;

        private MatchQueryTask(List<SegmentWriter> writers,
                               WavePattern query,
                               String queryId,
                               int topK,
                               int from,
                               int to,
                               int threshold) {
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

    private final class DetailedMatchQueryTask extends QueryTask<HeapItemDetailed> {
        private final WavePattern query;
        private final String queryId;
        private final int topK;

        private DetailedMatchQueryTask(List<SegmentWriter> writers,
                                       WavePattern query,
                                       String queryId,
                                       int topK,
                                       int from,
                                       int to,
                                       int threshold) {
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