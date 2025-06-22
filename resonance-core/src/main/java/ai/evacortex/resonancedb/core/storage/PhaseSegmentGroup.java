/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.storage.compactor.SegmentCompactor;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class PhaseSegmentGroup {

    private static final long MAX_SEG_BYTES = Long.parseLong(
            System.getProperty("segment.maxBytes", "" + (32L << 20)));
    private final AtomicInteger seq = new AtomicInteger();
    private volatile SegmentWriter current;
    private final String baseName;
    private final Path baseDir;
    private final CopyOnWriteArrayList<SegmentWriter> writers = new CopyOnWriteArrayList<>();
    private final SegmentCompactor compactor;
    private final ReentrantLock lock = new ReentrantLock();

    public PhaseSegmentGroup(String baseName, Path baseDir, SegmentCompactor compactor) {
        this.baseName = baseName;
        this.baseDir = baseDir;
        this.compactor = compactor;
        loadExistingSegments();
    }

    private void loadExistingSegments() {
        try {
            if (!Files.exists(baseDir)) return;
            Files.list(baseDir)
                    .filter(p -> p.getFileName().toString().startsWith(baseName + "-") &&
                            p.getFileName().toString().endsWith(".segment"))
                    .sorted()
                    .forEach(p -> writers.add(new SegmentWriter(p)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load segment group: " + baseName, e);
        }

        if (writers.isEmpty()) {
            writers.add(createSegment(seq.getAndIncrement()));   // seq=0
        } else {
            int lastIdx = writers.size();                        // size == maxIndex+1
            seq.set(lastIdx);
        }
        current = writers.getLast();
    }

    private SegmentWriter createSegment(int index) {
        try {
            String filename = baseName + "-" + index + ".segment";
            Path fullPath = baseDir.resolve(filename);
            Files.createDirectories(baseDir);
            if (Files.notExists(fullPath)) {
                Files.createFile(fullPath);
            }
            return new SegmentWriter(fullPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create segment: " + baseName + "-" + index, e);
        }
    }

    private SegmentWriter createSegment() {
        int index = seq.getAndIncrement();
        try {
            String filename = baseName + "-" + index + ".segment";
            Path fullPath = baseDir.resolve(filename);
            Files.createDirectories(baseDir);
            if (Files.notExists(fullPath)) Files.createFile(fullPath);
            return new SegmentWriter(fullPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create segment: " + baseName + "-" + index, e);
        }
    }

    public SegmentWriter getWritable() {
        lock.lock();
        try {
            if (current != null && current.approxSize() <= MAX_SEG_BYTES) {
                return current;
            }
            current = createSegment();          // ✨ вместо createSegment(nextIndex)
            writers.add(current);
            return current;
        } finally {
            lock.unlock();
        }
    }

    public SegmentWriter createAndRegisterNewSegment() {
        lock.lock();
        try {
            current = createSegment();          // ✨ seq гарантирует уникальность
            writers.add(current);
            return current;
        } finally {
            lock.unlock();
        }
    }

    public List<SegmentWriter> getAll() {
        return Collections.unmodifiableList(writers);
    }

    public List<Path> getPaths() {
        return writers.stream().map(SegmentWriter::getPath).collect(Collectors.toList());
    }

    public String getBaseName() {
        return baseName;
    }

    public void resetTo(SegmentWriter writer) {
        lock.lock();
        try {
            writers.clear();
            writers.add(writer);
            current = writer;                   // ✨

            // извлекаем индекс из имени «base-N.segment»
            String name = writer.getSegmentName();
            int idx = 0;
            try {
                int cut1 = baseName.length() + 1;
                idx = Integer.parseInt(name.substring(cut1, name.length() - 8)); // -8 = \".segment\"
            } catch (Exception ignore) {}
            seq.set(idx + 1);                   // ✨ следующий файл будет уникален
        } finally {
            lock.unlock();
        }
    }

    public double totalFillRatio() {
        return writers.stream().mapToDouble(SegmentWriter::getFillRatio).average().orElse(1.0);
    }

    public boolean shouldCompact() {
        return writers.size() > 3 && totalFillRatio() < 0.35;
    }

    public boolean maybeCompact() {
        if (shouldCompact()) {
            compactor.compact(this);
            return true;
        }
        return false;
    }

    public void registerIfAbsent(SegmentWriter writer) {
        writers.addIfAbsent(writer);
    }
}