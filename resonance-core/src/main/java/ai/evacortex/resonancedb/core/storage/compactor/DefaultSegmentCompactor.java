/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.compactor;

import ai.evacortex.resonancedb.core.metadata.PatternMetaStore;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;
import ai.evacortex.resonancedb.core.storage.PhaseSegmentGroup;
import ai.evacortex.resonancedb.core.storage.io.SegmentCache;
import ai.evacortex.resonancedb.core.storage.io.SegmentReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Stream;

public class DefaultSegmentCompactor implements SegmentCompactor {

    private static final int MAX_MOVE_ATTEMPTS = 10;

    private record PendingEntry(String id, String oldSegment, long oldOffset,
                                long newOffset, double phaseCenter) {}

    private final ManifestIndex manifest;
    private final PatternMetaStore metaStore;
    private final Path segmentDir;
    private final ReadWriteLock globalLock;
    private final SegmentCache readerCache;

    public DefaultSegmentCompactor(ManifestIndex manifest,
                                   PatternMetaStore metaStore,
                                   Path segmentDir,
                                   ReadWriteLock globalLock,
                                   SegmentCache readerCache) {
        this.manifest = manifest;
        this.metaStore = metaStore;
        this.segmentDir = segmentDir;
        this.globalLock = globalLock;
        this.readerCache = readerCache;
    }

    @Override
    public void compact(PhaseSegmentGroup group) {
        List<SegmentWriter> oldWriters;
        String finalName;
        Path tmpPath;
        Path finalPath;
        List<PendingEntry> pendingEntries;

        globalLock.writeLock().lock();
        try {
            oldWriters = new ArrayList<>(group.getAll());
            if (oldWriters.size() <= 1) return;

            String base = group.getBaseName();
            long timestamp = System.currentTimeMillis();
            String tmpName = base + "-tmp-merged-" + timestamp + ".segment";
            finalName = base + "-merged-" + timestamp + ".segment";
            tmpPath = segmentDir.resolve(tmpName);
            finalPath = segmentDir.resolve(finalName);

            cleanupOldTmpSegments(base);
            Files.createDirectories(segmentDir);
            SegmentWriter tmpWriter = new SegmentWriter(tmpPath);

            pendingEntries = new ArrayList<>();
            for (SegmentWriter writer : oldWriters) {
                try (SegmentReader reader = new SegmentReader(writer.getPath())) {
                    for (SegmentReader.PatternWithId entry : reader.readAllWithId()) {
                        ManifestIndex.PatternLocation loc = manifest.get(entry.id());
                        if (loc == null) continue;
                        if (!loc.segmentName().equals(writer.getSegmentName())) continue;
                        if (loc.offset() != entry.offset()) continue;
                        long newOff = tmpWriter.write(entry.id(), entry.pattern());
                        double phaseCenter = circularMeanPhase(entry.pattern().phase());
                        pendingEntries.add(new PendingEntry(entry.id(),
                                writer.getSegmentName(), entry.offset(), newOff, phaseCenter));
                    }
                }
            }

            tmpWriter.flush();
            tmpWriter.close();

            group.createAndRegisterNewSegment();
        } catch (IOException e) {
            throw new RuntimeException("Compaction failed: merge phase", e);
        } finally {
            globalLock.writeLock().unlock();
        }

        try {
            safeMoveWithRetry(tmpPath, finalPath);
        } catch (IOException e) {
            try { Files.deleteIfExists(tmpPath); } catch (IOException ignored) {}
            throw new RuntimeException("Compaction failed: file move", e);
        }

        globalLock.writeLock().lock();
        try {
            SegmentWriter mergedWriter = new SegmentWriter(finalPath);
            mergedWriter.sync();

            List<Double> phaseCenters = new ArrayList<>();
            for (PendingEntry pe : pendingEntries) {
                ManifestIndex.PatternLocation loc = manifest.get(pe.id);
                if (loc == null) continue;
                if (!loc.segmentName().equals(pe.oldSegment)) continue;
                if (loc.offset() != pe.oldOffset) continue;

                manifest.replace(pe.id, pe.oldSegment, pe.oldOffset,
                        finalName, pe.newOffset, pe.phaseCenter);
                phaseCenters.add(pe.phaseCenter);
            }

            Set<String> oldWriterNames = new HashSet<>();
            for (SegmentWriter w : oldWriters) {
                oldWriterNames.add(w.getSegmentName());
            }
            List<SegmentWriter> currentWriters = new ArrayList<>(group.getAll());

            group.registerIfAbsent(mergedWriter);
            group.resetTo(mergedWriter);

            for (SegmentWriter w : currentWriters) {
                if (!oldWriterNames.contains(w.getSegmentName())) {
                    group.registerIfAbsent(w);
                }
            }

            for (double pc : phaseCenters) {
                group.updatePhaseStats(pc);
            }
            readerCache.updateVersion(mergedWriter.getSegmentName(), mergedWriter.getWriteOffset());
            manifest.flush();
            metaStore.flush();

            for (SegmentWriter w : oldWriters) {
                String oldSegName = w.getSegmentName();
                readerCache.removeSegment(oldSegName);
                try {
                    w.close();
                    Files.deleteIfExists(w.getPath());
                } catch (Exception e) {
                    System.err.println("WARN compaction cleanup: " + oldSegName + ": " + e.getMessage());
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Compaction failed: metadata update", e);
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

    private static double circularMeanPhase(double[] phase) {
        double sinSum = 0.0, cosSum = 0.0;
        for (double p : phase) {
            sinSum += Math.sin(p);
            cosSum += Math.cos(p);
        }
        if (sinSum == 0.0 && cosSum == 0.0) return 0.0;
        return Math.atan2(sinSum, cosSum);
    }

    private void cleanupOldTmpSegments(String baseName) {
        try (Stream<Path> files = Files.list(segmentDir)) {
            files.filter(p -> p.getFileName().toString().startsWith(baseName + "-tmp-merged-"))
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); }
                        catch (IOException e) {
                            System.err.println("WARN tmp cleanup: " + p + ": " + e.getMessage());
                        }
                    });
        } catch (IOException e) {
            System.err.println("WARN listDir for tmp cleanup: " + segmentDir + ": " + e.getMessage());
        }
    }
}
