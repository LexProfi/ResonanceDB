/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.compactor;

import ai.evacortex.resonancedb.core.metadata.PatternMetaStore;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;
import ai.evacortex.resonancedb.core.storage.PhaseSegmentGroup;
import ai.evacortex.resonancedb.core.storage.io.SegmentReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

public class DefaultSegmentCompactor implements SegmentCompactor {

    private final ManifestIndex manifest;
    private final PatternMetaStore metaStore;
    private final Path segmentDir;
    private final ReadWriteLock globalLock;

    public DefaultSegmentCompactor(ManifestIndex manifest,
                                   PatternMetaStore metaStore,
                                   Path segmentDir,
                                   ReadWriteLock globalLock) {
        this.manifest = manifest;
        this.metaStore = metaStore;
        this.segmentDir = segmentDir;
        this.globalLock = globalLock;
    }

    @Override
    public void compact(PhaseSegmentGroup group) {
        globalLock.writeLock().lock();
        try {
            List<SegmentWriter> oldWriters = new ArrayList<>(group.getAll());
            if (oldWriters.size() <= 1) return;

            String base       = group.getBaseName();
            String mergedName = base + "-merged-" + System.currentTimeMillis() + ".segment";
            Path   mergedPath = segmentDir.resolve(mergedName);

            Files.createDirectories(segmentDir);
            SegmentWriter mergedWriter = new SegmentWriter(mergedPath);

            group.registerIfAbsent(mergedWriter);

            for (SegmentWriter writer : oldWriters) {
                try (SegmentReader reader = new SegmentReader(writer.getPath())) {
                    for (SegmentReader.PatternWithId entry : reader.readAllWithId()) {
                        ManifestIndex.PatternLocation loc = manifest.get(entry.id());
                        if (loc == null) continue;
                        if (!loc.segmentName().equals(writer.getSegmentName())) continue;
                        if (loc.offset() != entry.offset()) continue;

                        long newOff = mergedWriter.write(entry.id(), entry.pattern());

                        manifest.replace(entry.id(),
                                writer.getSegmentName(), entry.offset(),
                                mergedWriter.getSegmentName(), newOff,
                                loc.phaseCenter());
                    }
                }
            }

            mergedWriter.flush();
            mergedWriter.sync();
            group.resetTo(mergedWriter);
            manifest.flush();
            metaStore.flush();

            for (SegmentWriter w : oldWriters) {
                try { w.close(); Files.deleteIfExists(w.getPath()); }
                catch (Exception ignore) { }
            }
        } catch (IOException e) {
            throw new RuntimeException("Compaction failed", e);
        } finally {
            globalLock.writeLock().unlock();
        }
    }
}