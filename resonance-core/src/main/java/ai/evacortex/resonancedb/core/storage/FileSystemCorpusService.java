/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.corpus.CorpusInfo;
import ai.evacortex.resonancedb.core.corpus.CorpusService;
import ai.evacortex.resonancedb.core.corpus.CorpusSpec;
import ai.evacortex.resonancedb.core.corpus.CorpusState;
import ai.evacortex.resonancedb.core.ResonanceStore;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.io.SegmentReader;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceEntry;
import ai.evacortex.resonancedb.core.storage.responce.InterferenceMap;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatchDetailed;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FileSystemCorpusService implements CorpusService, Closeable {

    private static final String CORPORA_DIR_NAME = "corpora";
    private static final String CORPUS_META_FILE = "corpus.json";
    private static final String LEGACY_MANIFEST = "index/manifest.idx";
    private static final String LEGACY_SEGMENTS = "segments";
    private static final String LEGACY_METADATA = "metadata/pattern-meta.json";
    private static final int DEFAULT_PATTERN_LEN = 1536;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path dbRoot;
    private final Path corporaRoot;
    private final StoreRuntimeServices runtime;
    private final boolean ownRuntime;
    private final String defaultCorpusId;
    private final boolean legacyDefaultEnabled;
    private final int maxOpenCorpora;
    private final long idleCloseNanos;

    private final ConcurrentMap<String, CorpusSlot> slots = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean rootsInitialized = new AtomicBoolean(false);

    private final ScheduledFuture<?> sweepTask;

    public FileSystemCorpusService(Path dbRoot) {
        this(dbRoot, StoreRuntimeServices.fromSystemProperties(), true);
    }

    public FileSystemCorpusService(Path dbRoot,
                                   StoreRuntimeServices runtime,
                                   boolean ownRuntime) {
        this.dbRoot = Objects.requireNonNull(dbRoot, "dbRoot must not be null")
                .toAbsolutePath()
                .normalize();
        this.corporaRoot = this.dbRoot.resolve(CORPORA_DIR_NAME);
        this.runtime = Objects.requireNonNull(runtime, "runtime must not be null");
        this.ownRuntime = ownRuntime;

        this.defaultCorpusId = CorpusService.normalizeCorpusId(
                System.getProperty("resonance.corpus.defaultId", "default")
        );
        this.legacyDefaultEnabled = Boolean.parseBoolean(
                System.getProperty("resonance.corpus.legacy.default.enabled", "true")
        );
        this.maxOpenCorpora = Math.max(
                8,
                Integer.getInteger(
                        "resonance.corpus.maxOpen",
                        Math.max(32, Runtime.getRuntime().availableProcessors() * 4)
                )
        );

        long idleCloseSeconds = Math.max(
                30L,
                Long.getLong("resonance.corpus.idleClose.seconds", 600L)
        );
        this.idleCloseNanos = TimeUnit.SECONDS.toNanos(idleCloseSeconds);

        ensureStorageRoots();

        this.sweepTask = runtime.scheduler().scheduleAtFixedRate(
                this::sweepIdleCorpora,
                30, 30, TimeUnit.SECONDS
        );
    }

    @Override
    public ResonanceStore store(String corpusId) {
        ensureOpen();
        ensureStorageRoots();
        String normalized = CorpusService.normalizeCorpusId(corpusId);
        return slots.computeIfAbsent(normalized, CorpusSlot::new).proxy;
    }

    @Override
    public Optional<CorpusInfo> info(String corpusId) {
        ensureOpen();
        ensureStorageRoots();

        String normalized = CorpusService.normalizeCorpusId(corpusId);

        CorpusSlot slot = slots.get(normalized);
        if (slot != null) {
            CorpusInfo live = slot.peekInfo();
            if (live != null) {
                return Optional.of(live);
            }
        }

        return Optional.ofNullable(loadInfoIfPresent(normalized));
    }

    @Override
    public boolean exists(String corpusId) {
        ensureOpen();
        ensureStorageRoots();

        String normalized = CorpusService.normalizeCorpusId(corpusId);

        CorpusSlot slot = slots.get(normalized);
        if (slot != null && slot.existsOnDisk()) {
            return true;
        }
        return existsOnDisk(normalized);
    }

    @Override
    public List<CorpusInfo> list() {
        ensureOpen();
        ensureStorageRoots();

        TreeSet<String> ids = new TreeSet<>();
        ids.addAll(slots.keySet());
        ids.addAll(scanPersistedCorpusIds());

        List<CorpusInfo> out = new ArrayList<>(ids.size());
        for (String id : ids) {
            info(id).ifPresent(out::add);
        }
        return List.copyOf(out);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        sweepTask.cancel(false);

        List<CorpusSlot> openSlots = new ArrayList<>(slots.values());
        for (CorpusSlot slot : openSlots) {
            slot.closeOpenStore();
        }

        if (ownRuntime) {
            runtime.close();
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("CorpusService is already closed");
        }
    }

    /**
     * Ensures that the base storage roots exist.
     *
     * <p>This is intentionally idempotent and may be called both eagerly from the
     * constructor and lazily from operational paths. If the directories were deleted
     * after startup, they will be recreated.</p>
     */
    private void ensureStorageRoots() {
        if (rootsInitialized.get()) {
            if (Files.isDirectory(dbRoot) && Files.isDirectory(corporaRoot)) {
                return;
            }
        }

        try {
            Files.createDirectories(dbRoot);
            Files.createDirectories(corporaRoot);
            rootsInitialized.set(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize corpus storage roots under: " + dbRoot, e);
        }
    }

    private void ensureCorpusDirectories(Path corpusRoot) {
        try {
            Files.createDirectories(corpusRoot);
            Files.createDirectories(corpusRoot.resolve("segments"));
            Files.createDirectories(corpusRoot.resolve("index"));
            Files.createDirectories(corpusRoot.resolve("metadata"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create corpus directories for root: " + corpusRoot, e);
        }
    }

    private void sweepIdleCorpora() {
        if (closed.get()) {
            return;
        }

        try {
            long now = System.nanoTime();
            long idleBefore = now - idleCloseNanos;

            List<CorpusSlot> open = slots.values().stream()
                    .filter(CorpusSlot::isOpen)
                    .sorted(Comparator.comparingLong(CorpusSlot::lastAccessNanos))
                    .collect(Collectors.toList());

            int openCount = open.size();

            for (CorpusSlot slot : open) {
                boolean overCapacity = openCount > maxOpenCorpora;
                boolean idleExpired = slot.lastAccessNanos() < idleBefore;

                if ((overCapacity || idleExpired) && slot.tryEvict()) {
                    openCount--;
                }
            }
        } catch (Throwable t) {
            System.err.println("[corpus-service] idle sweep failed: " + t.getMessage());
        }
    }

    private Set<String> scanPersistedCorpusIds() {
        LinkedHashSet<String> ids = new LinkedHashSet<>();

        if (legacyDefaultEnabled && isLegacyDefaultPresent()) {
            ids.add(defaultCorpusId);
        }

        if (!Files.isDirectory(corporaRoot)) {
            return ids;
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(corporaRoot)) {
            for (Path p : ds) {
                if (!Files.isDirectory(p)) {
                    continue;
                }
                String id = p.getFileName().toString();
                try {
                    ids.add(CorpusService.normalizeCorpusId(id));
                } catch (IllegalArgumentException ignored) {
                    // ignore foreign/non-corpus directories
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan corpus directories: " + corporaRoot, e);
        }

        return ids;
    }

    private boolean existsOnDisk(String corpusId) {
        Path root = resolveCorpusRoot(corpusId);

        if (!Files.exists(root)) {
            return false;
        }

        return Files.exists(root.resolve(CORPUS_META_FILE))
                || Files.exists(root.resolve(LEGACY_MANIFEST))
                || Files.isDirectory(root.resolve(LEGACY_SEGMENTS));
    }

    private CorpusInfo loadInfoIfPresent(String corpusId) {
        StoredCorpusMeta meta = loadOrRecoverCorpusMeta(corpusId);
        return meta != null ? meta.toInfo() : null;
    }

    private StoredCorpusMeta loadOrRecoverCorpusMeta(String corpusId) {
        Path root = resolveCorpusRoot(corpusId);
        Path metaPath = root.resolve(CORPUS_META_FILE);

        if (Files.exists(metaPath)) {
            return readMeta(metaPath);
        }

        if (!existsOnDisk(corpusId)) {
            return null;
        }

        StoredCorpusMeta recovered = recoverMeta(corpusId, root);
        if (recovered != null) {
            writeMeta(metaPath, recovered);
        }
        return recovered;
    }

    private StoredCorpusMeta recoverMeta(String corpusId, Path root) {
        int patternLength = detectPatternLength(root);
        if (patternLength <= 0) {
            patternLength = Integer.getInteger("resonance.pattern.len", DEFAULT_PATTERN_LEN);
        }

        long patternCount = readManifestCount(root.resolve(LEGACY_MANIFEST));

        Instant[] times = probeTimes(root);
        Instant createdAt = times[0];
        Instant updatedAt = times[1];

        return new StoredCorpusMeta(
                corpusId,
                patternLength,
                patternCount,
                CorpusState.ACTIVE.name(),
                createdAt.toEpochMilli(),
                updatedAt.toEpochMilli()
        );
    }

    private int detectPatternLength(Path corpusRoot) {
        Path segmentsDir = corpusRoot.resolve(LEGACY_SEGMENTS);
        if (!Files.isDirectory(segmentsDir)) {
            return -1;
        }

        try (Stream<Path> stream = Files.list(segmentsDir).sorted()) {
            List<Path> segments = stream
                    .filter(p -> p.getFileName().toString().endsWith(".segment"))
                    .toList();

            for (Path seg : segments) {
                try (SegmentReader reader = new SegmentReader(seg)) {
                    List<SegmentReader.PatternWithId> all = reader.readAllWithId();
                    if (!all.isEmpty()) {
                        return all.get(0).pattern().amplitude().length;
                    }
                } catch (RuntimeException ignored) {
                    // continue probing next segment
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to detect pattern length for corpus root: " + corpusRoot, e);
        }

        return -1;
    }

    private long readManifestCount(Path manifestPath) {
        if (!Files.exists(manifestPath)) {
            return 0L;
        }

        ManifestIndex manifest = ManifestIndex.loadOrCreate(manifestPath);
        return manifest.getAllLocations().size();
    }

    private Instant[] probeTimes(Path root) {
        Instant now = Instant.now();
        Instant createdAt = now;
        Instant updatedAt = now;

        List<Path> candidates = List.of(
                root,
                root.resolve(CORPUS_META_FILE),
                root.resolve(LEGACY_MANIFEST),
                root.resolve(LEGACY_SEGMENTS),
                root.resolve(LEGACY_METADATA)
        );

        Instant min = null;
        Instant max = null;

        for (Path p : candidates) {
            try {
                if (!Files.exists(p)) {
                    continue;
                }
                FileTime ft = Files.getLastModifiedTime(p);
                Instant t = ft.toInstant();
                if (min == null || t.isBefore(min)) {
                    min = t;
                }
                if (max == null || t.isAfter(max)) {
                    max = t;
                }
            } catch (IOException ignored) {
            }
        }

        if (min != null) {
            createdAt = min;
        }
        if (max != null) {
            updatedAt = max;
        }

        if (updatedAt.isBefore(createdAt)) {
            updatedAt = createdAt;
        }

        return new Instant[]{createdAt, updatedAt};
    }

    private StoredCorpusMeta readMeta(Path metaPath) {
        try (InputStream in = Files.newInputStream(metaPath)) {
            StoredCorpusMeta meta = MAPPER.readValue(in, StoredCorpusMeta.class);
            if (meta == null || meta.id == null || meta.id.isBlank()) {
                throw new IllegalStateException("Invalid corpus metadata: " + metaPath);
            }
            return meta;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read corpus metadata: " + metaPath, e);
        }
    }

    private void writeMeta(Path metaPath, StoredCorpusMeta meta) {
        try {
            Files.createDirectories(metaPath.getParent());

            Path tmp = metaPath.resolveSibling(metaPath.getFileName().toString() + ".tmp");
            try (OutputStream out = Files.newOutputStream(
                    tmp,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            )) {
                MAPPER.writerWithDefaultPrettyPrinter().writeValue(out, meta);
            }

            try {
                Files.move(tmp, metaPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, metaPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write corpus metadata: " + metaPath, e);
        }
    }

    private Path resolveCorpusRoot(String corpusId) {
        ensureStorageRoots();

        if (legacyDefaultEnabled
                && defaultCorpusId.equals(corpusId)
                && isLegacyDefaultPresent()) {
            return dbRoot;
        }
        return corporaRoot.resolve(corpusId);
    }

    private boolean isLegacyDefaultPresent() {
        return Files.exists(dbRoot.resolve(LEGACY_MANIFEST))
                || Files.isDirectory(dbRoot.resolve(LEGACY_SEGMENTS))
                || Files.exists(dbRoot.resolve(LEGACY_METADATA));
    }

    private final class CorpusSlot {
        private final String corpusId;
        private final ReentrantLock lock = new ReentrantLock();
        private final AtomicInteger activeOps = new AtomicInteger();
        private final AtomicLong lastAccessNanos = new AtomicLong(System.nanoTime());
        private final ResonanceStore proxy = new CorpusBoundStore(this);

        private volatile WavePatternStoreImpl store;
        private volatile CorpusInfo info;
        private volatile Throwable lastOpenFailure;

        private CorpusSlot(String corpusId) {
            this.corpusId = corpusId;
        }

        private CorpusInfo peekInfo() {
            CorpusInfo local = info;
            if (local != null) {
                return local;
            }

            StoredCorpusMeta meta = loadOrRecoverCorpusMeta(corpusId);
            if (meta == null) {
                return null;
            }

            CorpusInfo loaded = meta.toInfo();
            info = loaded;
            return loaded;
        }

        private boolean existsOnDisk() {
            return FileSystemCorpusService.this.existsOnDisk(corpusId);
        }

        private boolean isOpen() {
            return store != null;
        }

        private long lastAccessNanos() {
            return lastAccessNanos.get();
        }

        private void beginAccess() {
            ensureOpen();
            activeOps.incrementAndGet();
            lastAccessNanos.set(System.nanoTime());
        }

        private void endAccess() {
            lastAccessNanos.set(System.nanoTime());
            activeOps.decrementAndGet();
        }

        private boolean tryEvict() {
            if (activeOps.get() != 0) {
                return false;
            }
            closeOpenStore();
            return true;
        }

        private void closeOpenStore() {
            lock.lock();
            try {
                WavePatternStoreImpl local = store;
                store = null;
                if (local != null) {
                    local.close();
                }
            } finally {
                lock.unlock();
            }
        }

        private WavePatternStoreImpl openForRead() {
            WavePatternStoreImpl local = store;
            if (local != null) {
                return local;
            }

            lock.lock();
            try {
                local = store;
                if (local != null) {
                    return local;
                }

                StoredCorpusMeta meta = loadOrRecoverCorpusMeta(corpusId);
                if (meta == null) {
                    return null;
                }

                local = new WavePatternStoreImpl(
                        resolveCorpusRoot(corpusId),
                        meta.patternLength,
                        runtime
                );

                store = local;
                info = meta.toInfo();
                lastOpenFailure = null;
                return local;

            } catch (Throwable t) {
                lastOpenFailure = t;
                CorpusInfo existing = peekInfo();
                if (existing != null) {
                    info = existing.withState(CorpusState.BROKEN, Instant.now());
                }
                throw t;
            } finally {
                lock.unlock();
            }
        }

        private WavePatternStoreImpl openForWrite(WavePattern firstPatternIfNeeded) {
            WavePatternStoreImpl local = store;
            if (local != null) {
                return local;
            }

            lock.lock();
            try {
                local = store;
                if (local != null) {
                    return local;
                }

                StoredCorpusMeta meta = loadOrRecoverCorpusMeta(corpusId);
                if (meta == null) {
                    Objects.requireNonNull(firstPatternIfNeeded, "firstPatternIfNeeded must not be null");

                    CorpusSpec spec = CorpusSpec.fromFirstWrite(corpusId, firstPatternIfNeeded);
                    Instant now = Instant.now();

                    meta = new StoredCorpusMeta(
                            spec.id(),
                            spec.patternLength(),
                            0L,
                            CorpusState.ACTIVE.name(),
                            now.toEpochMilli(),
                            now.toEpochMilli()
                    );

                    Path root = resolveCorpusRoot(corpusId);
                    ensureCorpusDirectories(root);
                    writeMeta(root.resolve(CORPUS_META_FILE), meta);
                }

                local = new WavePatternStoreImpl(
                        resolveCorpusRoot(corpusId),
                        meta.patternLength,
                        runtime
                );

                store = local;
                info = meta.toInfo();
                lastOpenFailure = null;
                return local;

            } catch (Throwable t) {
                lastOpenFailure = t;
                CorpusInfo existing = peekInfo();
                if (existing != null) {
                    info = existing.withState(CorpusState.BROKEN, Instant.now());
                }
                throw t;
            } finally {
                lock.unlock();
            }
        }

        private float compareWithoutMaterialization(WavePattern a, WavePattern b) {
            WavePatternStoreImpl local = store;
            if (local != null) {
                return local.compare(a, b);
            }

            StoredCorpusMeta meta = loadOrRecoverCorpusMeta(corpusId);
            if (meta != null) {
                if (a == null || b == null) {
                    throw new NullPointerException("WavePattern arguments must not be null");
                }

                int lenA = a.amplitude().length;
                int lenB = b.amplitude().length;
                if (lenA != meta.patternLength || lenB != meta.patternLength) {
                    throw new IllegalArgumentException(
                            "Pattern length mismatch for corpus '" + corpusId +
                                    "': expected=" + meta.patternLength +
                                    ", got a=" + lenA + ", b=" + lenB
                    );
                }
            }

            return runtime.resonanceKernel().compare(a, b);
        }

        private void afterInsert() {
            mutateInfoCount(+1L);
        }

        private void afterDelete() {
            mutateInfoCount(-1L);
        }

        private void afterReplace() {
            CorpusInfo current = peekInfo();
            if (current == null) {
                return;
            }

            Instant now = Instant.now();
            CorpusInfo updated = current.withPatternCount(current.patternCount(), now);
            info = updated;
            writeMeta(resolveCorpusRoot(corpusId).resolve(CORPUS_META_FILE), StoredCorpusMeta.from(updated));
        }

        private void mutateInfoCount(long delta) {
            CorpusInfo current = peekInfo();
            if (current == null) {
                return;
            }

            Instant now = Instant.now();
            long nextCount = Math.max(0L, current.patternCount() + delta);
            CorpusInfo updated = current.withPatternCount(nextCount, now);
            info = updated;
            writeMeta(resolveCorpusRoot(corpusId).resolve(CORPUS_META_FILE), StoredCorpusMeta.from(updated));
        }
    }

    private static final class CorpusBoundStore implements ResonanceStore {

        private final CorpusSlot slot;

        private CorpusBoundStore(CorpusSlot slot) {
            this.slot = slot;
        }

        @Override
        public String insert(WavePattern psi, Map<String, String> metadata) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForWrite(psi);
                String id = store.insert(psi, metadata == null ? Map.of() : metadata);
                slot.afterInsert();
                return id;
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public void delete(String id) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForRead();
                if (store == null) {
                    throw new PatternNotFoundException(id);
                }
                store.delete(id);
                slot.afterDelete();
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public String replace(String id, WavePattern psi, Map<String, String> metadata) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForRead();
                if (store == null) {
                    throw new PatternNotFoundException(id);
                }
                String newId = store.replace(id, psi, metadata == null ? Map.of() : metadata);
                slot.afterReplace();
                return newId;
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public float compare(WavePattern a, WavePattern b) {
            slot.beginAccess();
            try {
                return slot.compareWithoutMaterialization(a, b);
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public List<ResonanceMatch> query(WavePattern query, int topK) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForRead();
                return store == null ? List.of() : store.query(query, topK);
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public List<ResonanceMatchDetailed> queryDetailed(WavePattern query, int topK) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForRead();
                return store == null ? List.of() : store.queryDetailed(query, topK);
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public InterferenceMap queryInterference(WavePattern query, int topK) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForRead();
                return store == null
                        ? new InterferenceMap(query, List.of())
                        : store.queryInterference(query, topK);
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public List<InterferenceEntry> queryInterferenceMap(WavePattern query, int topK) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForRead();
                return store == null ? List.of() : store.queryInterferenceMap(query, topK);
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public List<ResonanceMatch> queryComposite(List<WavePattern> patterns, List<Double> weights, int topK) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForRead();
                return store == null ? List.of() : store.queryComposite(patterns, weights, topK);
            } finally {
                slot.endAccess();
            }
        }

        @Override
        public List<ResonanceMatchDetailed> queryCompositeDetailed(List<WavePattern> patterns, List<Double> weights, int topK) {
            slot.beginAccess();
            try {
                WavePatternStoreImpl store = slot.openForRead();
                return store == null ? List.of() : store.queryCompositeDetailed(patterns, weights, topK);
            } finally {
                slot.endAccess();
            }
        }
    }

    /**
     * Small disk-backed corpus metadata record.
     *
     * <p>Public fields are intentional to keep Jackson usage friction-free and avoid
     * extra reflection requirements or record-module surprises.</p>
     */
    public static final class StoredCorpusMeta {
        public String id;
        public int patternLength;
        public long patternCount;
        public String state;
        public long createdAtEpochMillis;
        public long updatedAtEpochMillis;

        public StoredCorpusMeta() {
        }

        public StoredCorpusMeta(String id,
                                int patternLength,
                                long patternCount,
                                String state,
                                long createdAtEpochMillis,
                                long updatedAtEpochMillis) {
            this.id = id;
            this.patternLength = patternLength;
            this.patternCount = patternCount;
            this.state = state;
            this.createdAtEpochMillis = createdAtEpochMillis;
            this.updatedAtEpochMillis = updatedAtEpochMillis;
        }

        public CorpusInfo toInfo() {
            CorpusState corpusState;
            try {
                corpusState = CorpusState.valueOf(state);
            } catch (Exception e) {
                corpusState = CorpusState.BROKEN;
            }

            return new CorpusInfo(
                    new CorpusSpec(id, patternLength),
                    corpusState,
                    Math.max(0L, patternCount),
                    Instant.ofEpochMilli(createdAtEpochMillis),
                    Instant.ofEpochMilli(Math.max(createdAtEpochMillis, updatedAtEpochMillis))
            );
        }

        public static StoredCorpusMeta from(CorpusInfo info) {
            return new StoredCorpusMeta(
                    info.id(),
                    info.patternLength(),
                    info.patternCount(),
                    info.state().name(),
                    info.createdAt().toEpochMilli(),
                    info.updatedAt().toEpochMilli()
            );
        }
    }
}