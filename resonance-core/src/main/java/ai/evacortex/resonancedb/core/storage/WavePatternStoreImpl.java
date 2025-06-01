/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.ResonanceMatch;
import ai.evacortex.resonancedb.core.ResonanceStore;
import ai.evacortex.resonancedb.core.WavePattern;
import ai.evacortex.resonancedb.core.engine.ResonanceEngine;
import ai.evacortex.resonancedb.core.exceptions.DuplicatePatternException;
import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.metadata.PatternMetaStore;
import ai.evacortex.resonancedb.core.sharding.PhaseShardSelector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


/**
 * WavePatternStoreImpl is the main implementation of the ResonanceStore interface,
 * responsible for managing insertions, deletions, updates, and queries of WavePatterns,
 * with support for phase-based sharding using a PhaseShardSelector.
 */
public class WavePatternStoreImpl implements ResonanceStore {

    private final ManifestIndex manifest;
    private final PatternMetaStore metaStore;
    private final Map<String, SegmentWriter> segmentWriters;
    private final Path rootDir;
    private final PhaseShardSelector shardSelector;
    private final ResonanceTracer tracer;

    public WavePatternStoreImpl(Path dbRoot) {
        this.rootDir = dbRoot;
        this.manifest = ManifestIndex.loadOrCreate(dbRoot.resolve("index/manifest.idx"));
        this.metaStore = PatternMetaStore.loadOrCreate(dbRoot.resolve("metadata/pattern-meta.json"));
        this.segmentWriters = new ConcurrentHashMap<>();
        this.tracer = new NoOpTracer(); // Can be replaced with full tracing

        // Load writers for all known segments from manifest
        for (String segmentName : manifest.getAllSegmentNames()) {
            Path segPath = dbRoot.resolve("segments/" + segmentName);
            segmentWriters.put(segmentName, new SegmentWriter(segPath));
        }

        // Reconstruct sharding map from manifest index entries
        List<ManifestIndex.PatternLocation> locations = manifest.getAllLocations();
        this.shardSelector = PhaseShardSelector.fromManifest(locations, 0.1); // default epsilon = 0.1
    }

    @Override
    public synchronized void insert(String id, WavePattern psi, Map<String, String> metadata) {
        if (manifest.contains(id)) {
            throw new DuplicatePatternException(id);
        }

        SegmentWriter writer = getOrCreateWriterFor(psi);
        long offset = writer.write(id, psi, metadata);
        double phaseCenter = Arrays.stream(psi.phase()).average().orElse(0.0);

        try {
            manifest.add(id, writer.getSegmentName(), offset, phaseCenter);
            metaStore.put(id, metadata);
            metaStore.flush();
        } catch (Exception e) {
            manifest.remove(id); // rollback manifest if meta write fails
            throw new RuntimeException("Failed to persist metadata", e);
        }
    }

    @Override
    public synchronized String insert(WavePattern psi) {
        String id = HashingUtil.computeContentHash(psi);
        insert(id, psi, Map.of());
        return id;
    }

    @Override
    public synchronized void delete(String id) {
        ManifestIndex.PatternLocation loc = manifest.get(id);
        if (loc == null) {
            throw new PatternNotFoundException(id);
        }

        SegmentWriter writer = segmentWriters.get(loc.segmentName());
        if (writer == null) {
            throw new RuntimeException("Segment writer not found for: " + loc.segmentName());
        }

        writer.markDeleted(loc.offset());
        manifest.remove(id);
        metaStore.remove(id);
    }

    @Override
    public synchronized void update(String id, WavePattern psi) {
        if (!manifest.contains(id)) {
            throw new PatternNotFoundException(id);
        }
        delete(id);
        insert(id, psi, Map.of());
    }

    @Override
    public List<ResonanceMatch> query(WavePattern query, int topK) {
        PriorityQueue<ResonanceMatch> heap = new PriorityQueue<>(topK, Comparator.comparingDouble(ResonanceMatch::energy));

        for (String shard : shardSelector.getRelevantShards(query)) {
            SegmentWriter writer = segmentWriters.get(shard);
            if (writer == null) continue;

            try (SegmentReader reader = new SegmentReader(writer.getPath())) {
                for (SegmentReader.PatternWithId entry : reader.readAllWithId()) {
                    WavePattern candidate = entry.pattern();
                    float score = ResonanceEngine.compare(query, candidate);

                    tracer.trace(entry.id(), query, candidate, score);

                    if (heap.size() < topK) {
                        heap.add(new ResonanceMatch(entry.id(), score, candidate));
                    } else if (score > heap.peek().energy()) {
                        heap.poll();
                        heap.add(new ResonanceMatch(entry.id(), score, candidate));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read segment: " + shard, e);
            }
        }

        return heap.stream()
                .sorted(Comparator.comparingDouble(ResonanceMatch::energy).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public float compare(WavePattern a, WavePattern b) {
        return ResonanceEngine.compare(a, b);
    }

    private SegmentWriter getOrCreateWriterFor(WavePattern psi) {
        String segmentName = shardSelector.selectShard(psi);
        return segmentWriters.computeIfAbsent(segmentName, name -> {
            try {
                Path path = rootDir.resolve("segments/" + name);
                Files.createDirectories(path.getParent());
                return new SegmentWriter(path);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create segment writer: " + name, e);
            }
        });
    }
}
