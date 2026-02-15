/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
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
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Stream;

public class DefaultSegmentCompactor implements SegmentCompactor {

    private static final int MAX_MOVE_ATTEMPTS = 10;

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

            String base = group.getBaseName();
            long timestamp = System.currentTimeMillis();
            String tmpName = base + "-tmp-merged-" + timestamp + ".segment";
            String finalName = base + "-merged-" + timestamp + ".segment";
            Path tmpPath = segmentDir.resolve(tmpName);
            Path finalPath = segmentDir.resolve(finalName);

            cleanupOldTmpSegments(base);
            Files.createDirectories(segmentDir);
            SegmentWriter tmpWriter = new SegmentWriter(tmpPath);

            for (SegmentWriter writer : oldWriters) {
                try (SegmentReader reader = new SegmentReader(writer.getPath())) {
                    for (SegmentReader.PatternWithId entry : reader.readAllWithId()) {
                        ManifestIndex.PatternLocation loc = manifest.get(entry.id());
                        if (loc == null) continue;
                        if (!loc.segmentName().equals(writer.getSegmentName())) continue;
                        if (loc.offset() != entry.offset()) continue;
                        long newOff = tmpWriter.write(entry.id(), entry.pattern());
                        manifest.replace(entry.id(),
                                writer.getSegmentName(), entry.offset(),
                                finalName, newOff,
                                loc.phaseCenter());
                    }
                }
            }

            tmpWriter.flush();
            tmpWriter.close();
            safeMoveWithRetry(tmpPath, finalPath);
            SegmentWriter mergedWriter = new SegmentWriter(finalPath);
            mergedWriter.sync();
            group.registerIfAbsent(mergedWriter);
            group.resetTo(mergedWriter);
            manifest.flush();
            metaStore.flush();

            for (SegmentWriter w : oldWriters) {
                try {
                    w.close();
                    Files.deleteIfExists(w.getPath());
                } catch (Exception ignore) {}
            }

        } catch (IOException e) {
            throw new RuntimeException("Compaction failed", e);
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    private static void safeMoveWithRetry(Path source, Path target) throws IOException {
        int attempt = 0;
        long delay = 100;
        while (true) {
            try {
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
                return;
            } catch (IOException e) {
                attempt++;
                if (attempt >= DefaultSegmentCompactor.MAX_MOVE_ATTEMPTS) {
                    throw new IOException("Failed to move " + source + " to " + target + " after " + attempt
                            + " attempts", e);
                }
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Move interrupted", ie);
                }
                delay *= 2;
            }
        }
    }

    private void cleanupOldTmpSegments(String baseName) {
        try (Stream<Path> files = Files.list(segmentDir)) {
            files.filter(p -> p.getFileName().toString().startsWith(baseName + "-tmp-merged-"))
                    .forEach(p -> {try {Files.deleteIfExists(p);} catch (IOException _) {}});
        } catch (IOException _) {
        }
    }
}
