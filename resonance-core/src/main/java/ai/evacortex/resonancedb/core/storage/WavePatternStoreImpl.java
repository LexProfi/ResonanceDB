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
import ai.evacortex.resonancedb.core.storage.io.SegmentReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentReaderCache;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WavePatternStoreImpl implements ResonanceStore, Closeable {


    // ==== segment-layout ====================================================
    /** ширина фазовой корзины (рад); 0.2 рад ⇒ 32 buckets по кругу */
    private static final double BUCKET_WIDTH_RAD = Double.parseDouble(System.getProperty("segment.bucketRad", "0.2"));
    /** максимальный размер одного .segment-файла (байт) — 32 MB */
    private static final long MAX_SEG_BYTES = Long.parseLong(System.getProperty("segment.maxBytes", "" + (32L << 20))); // 32 * 2^20
    // включено по умолчанию; отключить → -Dripple.enable=false
    private static final boolean RIPPLE_ENABLE = Boolean.parseBoolean(System.getProperty("ripple.enable", "true"));
    // стартовый радиус (0 = только центр)
    private static final int R0_SEGMENTS = Integer.parseInt(System.getProperty("ripple.radius0", "0"));
    // приращение кольца; 1 сегмент — оптимум по latency/recall
    private static final int STEP_SEGMENTS = Integer.parseInt(System.getProperty("ripple.step", "1"));
    // -1  → вычислить как shardCount/2 (вся окружность)
    private static final int MAX_RADIUS_SEG_CONFIG = Integer.parseInt(System.getProperty("ripple.maxRadius", "-1"));
    // порог затухания; 0.15 — эмпирический sweet-spot
    private static final float ENERGY_DAMP = Float.parseFloat(System.getProperty("ripple.energyDamp", "0.15"));
    private static final double READ_EPSILON  = 0.1;
    private static final float EXACT_MATCH_EPS = 1e-6f;
    private static final int OFFSET_TOLERANCE = 64;
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
    private final SegmentReaderCache readerCache;
    private record IdAndPattern(String id, WavePattern pattern) {}

    public WavePatternStoreImpl(Path dbRoot) {
        this.rootDir = dbRoot;
        this.manifest = ManifestIndex.loadOrCreate(dbRoot.resolve("index/manifest.idx"));
        this.manifest.ensureFileExists();
        this.metaStore = PatternMetaStore.loadOrCreate(dbRoot.resolve("metadata/pattern-meta.json"));
        this.segmentGroups = new ConcurrentHashMap<>();
        this.tracer = new NoOpTracer();
        this.compactor = new DefaultSegmentCompactor(manifest, metaStore, rootDir.resolve("segments"), globalLock);
        this.readerCache = new SegmentReaderCache(rootDir.resolve("segments"), 128);

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

    /* ======================================================================
     * INSERT
     * ====================================================================*/
    @Override
    public String insert(WavePattern psi, Map<String, String> metadata)
            throws DuplicatePatternException, InvalidWavePatternException {

        String idKey = HashingUtil.computeContentHash(psi);
        HashingUtil.parseAndValidateMd5(idKey);

        globalLock.writeLock().lock();
        try {
            /* ---- 0. уникальность ------------------------------------------------ */
            if (manifest.contains(idKey)) {
                throw new DuplicatePatternException(idKey);
            }

            /* ---- 1. выбор группы / сегмента ------------------------------------- */
            double phaseCenter = Arrays.stream(psi.phase()).average().orElse(0.0);
            int bucket   = (int) Math.floor((phaseCenter + Math.PI) / BUCKET_WIDTH_RAD);
            String base  = "phase-" + bucket;
            PhaseSegmentGroup group = getOrCreateGroup(base);

            double existingCenter = manifest.getAllLocations().stream()
                    .filter(l -> l.segmentName().startsWith(base))
                    .mapToDouble(ManifestIndex.PatternLocation::phaseCenter)
                    .average()
                    .orElse(phaseCenter);

            SegmentWriter writer = group.getWritable();
            boolean sizeOverflow  = writer == null || writer.approxSize() > MAX_SEG_BYTES;
            boolean phaseOverflow = Math.abs(phaseCenter - existingCenter) > 0.15
                    && (writer == null || writer.approxSize() > (4L << 20)); // 4 MB

            if (sizeOverflow || phaseOverflow) {
                writer = group.createAndRegisterNewSegment();
            }
            group.registerIfAbsent(writer);

            /* ---- 2. запись (с защитой от переполнения) --------------------------- */
            long offset;
            String segmentName = null; // ⟵ добавлено
            while (true) {
                try {
                    offset = writer.write(idKey, psi);
                    writer.flush();
                    writer.sync();
                    segmentName = writer.getSegmentName(); // ⟵ добавлено
                    readerCache.invalidate(segmentName);   // ⟵ добавлено
                    break; // успех
                } catch (SegmentOverflowException _e) {
                    writer = group.createAndRegisterNewSegment();
                    group.registerIfAbsent(writer);
                }
            }

            /* ---- 3. метаданные, manifest, финал --------------------------------- */
            manifest.add(idKey, writer.getSegmentName(), offset, phaseCenter);
            if (!metadata.isEmpty()) metaStore.put(idKey, metadata);

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

    /* ======================================================================
     * DELETE
     * ====================================================================*/
    @Override
    public void delete(String idKey) throws PatternNotFoundException {
        globalLock.writeLock().lock();
        try {
            HashingUtil.parseAndValidateMd5(idKey);
            ManifestIndex.PatternLocation loc = manifest.get(idKey);
            if (loc == null) throw new PatternNotFoundException(idKey);

            SegmentWriter writer = getOrCreateWriter(loc.segmentName());
            writer.markDeleted(loc.offset());
            readerCache.invalidate(loc.segmentName());

            manifest.remove(idKey);
            metaStore.remove(idKey);

            manifest.flush();               // ← добавлено
            metaStore.flush();
            rebuildShardSelector();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /* ======================================================================
     * REPLACE
     * ====================================================================*/
    @Override
    public String replace(String oldId,
                          WavePattern newPattern,
                          Map<String, String> newMetadata)
            throws PatternNotFoundException, InvalidWavePatternException {

        if (newPattern.amplitude().length != newPattern.phase().length) {
            throw new InvalidWavePatternException("Amplitude / phase length mismatch");
        }

        HashingUtil.parseAndValidateMd5(oldId);
        String newId = HashingUtil.computeContentHash(newPattern);
        HashingUtil.parseAndValidateMd5(newId);

        globalLock.writeLock().lock();          // атомарная секция
        try {
            /* ---- 1. проверки ---------------------------------------------------- */
            ManifestIndex.PatternLocation oldLoc = manifest.get(oldId);
            if (oldLoc == null) throw new PatternNotFoundException(oldId);

            if (!oldId.equals(newId) && manifest.contains(newId)) {
                throw new DuplicatePatternException("Replacement would collide: " + newId);
            }

            /* ---- 2. выбор группы / сегмента ------------------------------------- */
            double phaseCenter = Arrays.stream(newPattern.phase()).average().orElse(0.0);
            int bucket   = (int) Math.floor((phaseCenter + Math.PI) / BUCKET_WIDTH_RAD);
            String base  = "phase-" + bucket;
            PhaseSegmentGroup group = getOrCreateGroup(base);

            SegmentWriter writer = group.getWritable();
            if (writer == null || writer.approxSize() > MAX_SEG_BYTES) {
                writer = group.createAndRegisterNewSegment();
            }
            group.registerIfAbsent(writer);

            /* ---- 3. запись нового паттерна с retry-циклом ----------------------- */
            long offset;
            while (true) {
                try {
                    offset = writer.write(newId, newPattern);
                    writer.flush();
                    writer.sync();
                    readerCache.invalidate(writer.getSegmentName());
                    break;
                } catch (SegmentOverflowException _e) {
                    writer = group.createAndRegisterNewSegment();
                    group.registerIfAbsent(writer);
                }
            }

// 👇👇👇 вот сюда вставить
            manifest.add(newId, writer.getSegmentName(), offset, phaseCenter);
            manifest.remove(oldId);

            if (!newMetadata.isEmpty()) metaStore.put(newId, newMetadata);

            if (!oldId.equals(newId)) {
                SegmentWriter oldWriter = getOrCreateWriter(oldLoc.segmentName());

                if (!(oldLoc.segmentName().equals(writer.getSegmentName()) && oldLoc.offset() == offset)) {
                    oldWriter.markDeleted(oldLoc.offset());
                }
                metaStore.remove(oldId);
            }
            manifest.flush();
            metaStore.flush();
            group.registerIfAbsent(writer);
            rebuildShardSelector();
            return newId;

        } finally {
            globalLock.writeLock().unlock();    // 🔓
        }
    }


    @Override
    public List<ResonanceMatch> query(WavePattern query, int topK) {
        globalLock.readLock().lock();
        try {
            if (!RIPPLE_ENABLE) {
                return legacyQuery(query, topK);
            }
            String qId = HashingUtil.computeContentHash(query);

            /* ---------- основной порядок ---------- */
            Comparator<HeapItem> orderAsc = Comparator
                    .comparingDouble(HeapItem::priority)
                    .thenComparing((HeapItem h) -> h.match().energy())
                    .thenComparing(h -> h.match().id());

            /* ---------- 1. «волновой» поиск ---------- */
            List<HeapItem> heap = rippleSearch(
                    query, topK,
                    (w, _) -> collectMatchesFromWriter(w, query, qId, topK),
                    orderAsc,
                    h -> h.match().energy());

            Set<String> seen = heap.stream()
                    .map(h -> h.match().id())
                    .collect(Collectors.toSet());

            for (ResonanceMatch m : legacyQuery(query, topK)) {
                if (seen.add(m.id())) {                 // ⚑ ID ещё не встречался
                    float pr = m.energy() + (m.id().equals(qId) ? 1.0f : 0f);
                    heap.add(new HeapItem(m, pr));
                }
            }

            // ---------- 2. fallback / дополнение ----------
            boolean selfFound = heap.stream()
                    .anyMatch(h -> h.match().id().equals(qId));

            if (heap.size() < topK || !selfFound) {
                for (ResonanceMatch m : legacyQuery(query, topK)) {
                    if (seen.add(m.id())) {                       // ← добавлено
                        float pr = m.energy() + (m.id().equals(qId) ? 1.0f : 0f);
                        heap.add(new HeapItem(m, pr));
                    }
                }
            }

            if (manifest.contains(qId) && heap.stream().noneMatch(h -> h.match().id().equals(qId))) {
                ManifestIndex.PatternLocation loc = manifest.get(qId);
                SegmentWriter w = snapshotWriters().get(loc.segmentName());
                if (w != null) {
                    for (HeapItem item : collectMatchesFromWriter(w, query, qId, topK)) {
                        if (item.match().id().equals(qId)) {
                            heap.add(item);
                            break;
                        }
                    }
                }
            }

            /* ---------- 3. окончательная выдача ---------- */
            return new ArrayList<>(                 // ← mutable
                    heap.stream()
                            .sorted(orderAsc.reversed())
                            .limit(topK)
                            .map(HeapItem::match)
                            .toList()
            );

        } finally {
            globalLock.readLock().unlock();
        }
    }

    public List<ResonanceMatch> legacyQuery(WavePattern query, int topK) {
        globalLock.readLock().lock();
        try {
            String queryId = HashingUtil.computeContentHash(query);
            Comparator<HeapItem> order = Comparator
                    .comparingDouble(HeapItem::priority).reversed()
                    .thenComparing((HeapItem h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<CompletableFuture<List<HeapItem>>> futures = segmentGroups.values().stream()
                    .flatMap(group -> Stream.concat(group.getAll().stream(),
                            Stream.ofNullable(group.getWritable())))
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
            if (!RIPPLE_ENABLE) {
                return legacyQueryDetailed(query, topK);
            }
            String qId = HashingUtil.computeContentHash(query);

            Comparator<HeapItemDetailed> orderAsc = Comparator
                    .comparingDouble(HeapItemDetailed::priority)
                    .thenComparing((HeapItemDetailed h) -> h.match().energy())
                    .thenComparing(h -> h.match().id());

            /* ---------- 1. «волновой» поиск ---------- */
            List<HeapItemDetailed> heap = rippleSearch(
                    query, topK,
                    (w, _seg) -> collectDetailedFromWriter(w, query, qId, topK),
                    orderAsc,
                    h -> h.match().energy());

            Set<String> seen = heap.stream()
                    .map(h -> h.match().id())
                    .collect(Collectors.toSet());

            for (ResonanceMatchDetailed m : legacyQueryDetailed(query, topK)) {
                if (seen.add(m.id())) {
                    double pr = m.energy() + (m.id().equals(qId) ? 1.0 : 0.0);
                    heap.add(new HeapItemDetailed(m, pr));
                }
            }

            // ---------- 2. резервное расширение ----------
            boolean selfFound = heap.stream()
                    .anyMatch(h -> h.match().id().equals(qId));

            if (heap.size() < topK || !selfFound) {
                for (ResonanceMatchDetailed m : legacyQueryDetailed(query, topK)) {
                    if (seen.add(m.id())) {                       // ← добавлено
                        double pr = m.energy() + (m.id().equals(qId) ? 1.0 : 0.0);
                        heap.add(new HeapItemDetailed(m, pr));
                    }
                }
            }
            if (manifest.contains(qId) && heap.stream().noneMatch(h -> h.match().id().equals(qId))) {
                ManifestIndex.PatternLocation loc = manifest.get(qId);
                SegmentWriter w = snapshotWriters().get(loc.segmentName());
                if (w != null) {
                    for (HeapItemDetailed item : collectDetailedFromWriter(w, query, qId, topK)) {
                        if (item.match().id().equals(qId)) {
                            heap.add(item);
                            break;
                        }
                    }
                }
            }
            /* ---------- 3. финальный список ---------- */
            return new ArrayList<>(                 // ← mutable
                    heap.stream()
                            .sorted(orderAsc.reversed())
                            .limit(topK)
                            .map(HeapItemDetailed::match)
                            .toList()
            );

        } finally {
            globalLock.readLock().unlock();
        }
    }

    public List<ResonanceMatchDetailed> legacyQueryDetailed(WavePattern query, int topK) {
        globalLock.readLock().lock();
        try {
            String queryId = HashingUtil.computeContentHash(query);

            Comparator<HeapItemDetailed> order = Comparator
                    .comparingDouble(HeapItemDetailed::priority).reversed()
                    .thenComparing((HeapItemDetailed h) -> h.match().energy(), Comparator.reverseOrder())
                    .thenComparing(h -> h.match().id());

            List<CompletableFuture<List<HeapItemDetailed>>> futures = segmentGroups.values().stream()
                    .flatMap(group -> Stream.concat(group.getAll().stream(),
                                                 Stream.ofNullable(group.getWritable())))
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

    private <T> List<T> rippleSearch(
            WavePattern query,
            int topK,
            BiFunction<SegmentWriter, String, List<T>> collector,
            Comparator<T> order,
            ToDoubleFunction<T> energyExtractor) {

        PhaseShardSelector sel = shardSelectorRef.get();
        List<String> ordered = sel.orderedShardList();
        String centerSeg = sel.selectShard(query);
        int centerIdx = sel.indexOf(centerSeg);
        int maxRadius = (MAX_RADIUS_SEG_CONFIG < 0)
                ? Math.max(ordered.size() - 1, 1)
                : MAX_RADIUS_SEG_CONFIG;
        Map<String, SegmentWriter> writerMap = snapshotWriters();
        Set<String> visited = new HashSet<>();
        PriorityQueue<T> pq = new PriorityQueue<>(order);

        int radius = R0_SEGMENTS;
        while (true) {
            List<String> scope = new ArrayList<>(2);
            if (radius == 0) {                       // первая волна
                if (writerMap.containsKey(centerSeg)) scope.add(centerSeg);
            } else {
                int left = centerIdx - radius;
                int right = centerIdx + radius;
                if (left >= 0) scope.add(ordered.get(left));
                if (right < ordered.size() && right != left) scope.add(ordered.get(right));
            }
            scope.removeIf(visited::contains);
            visited.addAll(scope);

            // параллельный сбор
            List<CompletableFuture<List<T>>> fut = scope.stream()
                    .map(name -> {
                        SegmentWriter w = writerMap.get(name);
                        return CompletableFuture.supplyAsync(() -> collector.apply(w, centerSeg), executor);
                    }).toList();

            fut.forEach(f -> f.join().forEach(item -> {
                pq.offer(item);
                if (pq.size() > topK) pq.poll();     // keep heap small
            }));

            // затухание
            if (!pq.isEmpty()) {
                double meanEnergy = pq.stream()
                        .mapToDouble(energyExtractor)
                        .average()
                        .orElse(0);
                if (meanEnergy < ENERGY_DAMP) break;
            }

            radius += STEP_SEGMENTS;
            if (radius > maxRadius) break;
            if (radius >= ordered.size() && visited.size() == writerMap.size()) break;
        }

        // fallback-π : сканируем остаток если всё ещё не topK
        if (pq.size() < topK) {
            List<CompletableFuture<List<T>>> rest = writerMap.entrySet().stream()
                    .filter(e -> !visited.contains(e.getKey()))
                    .map(e -> CompletableFuture.supplyAsync(() -> collector.apply(e.getValue(), centerSeg), executor))
                    .toList();
            rest.forEach(f -> f.join().forEach(item -> {
                pq.offer(item);
                if (pq.size() > topK) pq.poll();
            }));
        }

        return new ArrayList<>(
                pq.stream()
                        .sorted(order.reversed())
                        .limit(topK)
                        .toList()
        );
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

    private Map<String, SegmentWriter> snapshotWriters() {

        return segmentGroups.values().stream()
                .flatMap(g -> {                       // ← ДОБАВЛЕНО
                    Stream<SegmentWriter> base = g.getAll().stream();
                    SegmentWriter w = g.getWritable();
                    return (w == null) ? base : Stream.concat(base, Stream.of(w));
                })
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(
                                SegmentWriter::getSegmentName,
                                Function.identity(),
                                (a, b) -> a          // dedup по имени файла
                        ),
                        Collections::unmodifiableMap));
    }

    private List<HeapItem> collectMatchesFromWriter(SegmentWriter writer,
                                                    WavePattern query,
                                                    String queryId,
                                                    int topK) {
        if (writer == null) return List.of();

        CachedReader reader = readerCache.get(writer.getSegmentName());
        List<SegmentReader.PatternWithId> patterns = reader.readAllWithId();
        Map<String, SegmentReader.PatternWithId> latestValidEntries = new HashMap<>();

        for (SegmentReader.PatternWithId entry : patterns) {
            String id = entry.id();
            if (!manifest.contains(id)) continue;
            ManifestIndex.PatternLocation loc = manifest.get(id);
            if (loc == null || !loc.segmentName().equals(writer.getSegmentName())) continue;

            latestValidEntries.put(id, entry); // ⟵ никакого IdAndPattern
        }

        PriorityQueue<HeapItem> localHeap =
                new PriorityQueue<>(topK, Comparator.comparingDouble(HeapItem::priority));

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
    }

    private List<HeapItemDetailed> collectDetailedFromWriter(SegmentWriter writer,
                                                             WavePattern query,
                                                             String queryId,
                                                             int topK) {
        if (writer == null) return List.of();
        CachedReader reader = readerCache.get(writer.getSegmentName());

        List<SegmentReader.PatternWithId> patterns = reader.readAllWithId();
        Map<String, SegmentReader.PatternWithId> latestValidEntries = new HashMap<>();

        for (SegmentReader.PatternWithId entry : patterns) {
            String id = entry.id();
            if (!manifest.contains(id)) continue;
            ManifestIndex.PatternLocation loc = manifest.get(id);
            if (loc == null || !loc.segmentName().equals(writer.getSegmentName())) continue;

            latestValidEntries.put(id, entry);
        }

            PriorityQueue<HeapItemDetailed> localHeap = new PriorityQueue<>(topK, Comparator.comparingDouble(HeapItemDetailed::priority));

            for (SegmentReader.PatternWithId entry : latestValidEntries.values()) {
                WavePattern cand = entry.pattern();
                if (cand.amplitude().length != query.amplitude().length) continue;

                ComparisonResult result = ResonanceEngine.compareWithPhaseDelta(query, cand);
                float energy = result.energy();
                double phaseShift = result.phaseDelta();
                ResonanceZone zone = ResonanceZoneClassifier.classify(energy, phaseShift);
                                double zoneScore = switch (zone) {
                                    case CORE       -> 2.0;
                                    case FRINGE     -> 1.0;
                                    case SHADOW -> 0.0;
                                };

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

    public PatternMetaStore getMetaStore() {
        return metaStore;
    }

    private Map<String, CachedReader> snapshotReaders() {
        return segmentGroups.values().stream()
                .flatMap(g -> {
                    Stream<SegmentWriter> base = g.getAll().stream();
                    SegmentWriter w = g.getWritable();
                    return (w == null) ? base : Stream.concat(base, Stream.of(w));
                })
                .map(w -> Map.entry(w.getSegmentName(), readerCache.get(w.getSegmentName())))
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a),
                        Collections::unmodifiableMap));
    }
}