/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
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
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class PhaseSegmentGroup {

    private static final long MAX_SEG_BYTES = Long.parseLong(System.getProperty("resonance.segment.maxBytes", "" + (64L << 20)));
    private static final double PHASE_STATS_EPSILON = 1e-10;
    private final AtomicInteger seq = new AtomicInteger();
    private volatile SegmentWriter current;
    private final String baseName;
    private final Path baseDir;
    private final CopyOnWriteArrayList<SegmentWriter> writers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> sealedSegmentNames = new CopyOnWriteArrayList<>();
    private final SegmentCompactor compactor;
    private final ReentrantLock lock = new ReentrantLock();

    private double sinSum = 0.0;
    private double cosSum = 0.0;

    public PhaseSegmentGroup(String baseName, Path baseDir, SegmentCompactor compactor) {
        this.baseName = baseName;
        this.baseDir = baseDir;
        this.compactor = compactor;
        loadExistingSegments();
    }

    private void loadExistingSegments() {
        List<Path> segPaths = new java.util.ArrayList<>();
        try {
            if (!Files.exists(baseDir)) {
                writers.add(createSegment(seq.getAndIncrement()));
                current = writers.getLast();
                return;
            }
            try (var stream = Files.list(baseDir)) {
                stream.filter(p -> p.getFileName().toString().startsWith(baseName + "-") &&
                                p.getFileName().toString().endsWith(".segment"))
                        .sorted()
                        .forEach(segPaths::add);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load segment group: " + baseName, e);
        }

        if (segPaths.isEmpty()) {
            writers.add(createSegment(seq.getAndIncrement()));
        } else {
            for (int i = 0; i < segPaths.size() - 1; i++) {
                sealedSegmentNames.add(segPaths.get(i).getFileName().toString());
            }
            writers.add(new SegmentWriter(segPaths.getLast()));
            seq.set(segPaths.size());
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
            current = createSegment();
            writers.add(current);
            return current;
        } finally {
            lock.unlock();
        }
    }

    public SegmentWriter createAndRegisterNewSegment() {
        lock.lock();
        try {
            current = createSegment();
            writers.add(current);
            return current;
        } finally {
            lock.unlock();
        }
    }

    public List<SegmentWriter> getAll() {
        return List.copyOf(writers);
    }

    public List<String> getAllSegmentNames() {
        List<String> all = new java.util.ArrayList<>(sealedSegmentNames.size() + writers.size());
        all.addAll(sealedSegmentNames);
        for (SegmentWriter w : writers) {
            all.add(w.getSegmentName());
        }
        return all;
    }

    public List<Path> getPaths() {
        List<Path> paths = new java.util.ArrayList<>(sealedSegmentNames.size() + writers.size());
        for (String name : sealedSegmentNames) {
            paths.add(baseDir.resolve(name));
        }
        for (SegmentWriter w : writers) {
            paths.add(w.getPath());
        }
        return paths;
    }

    public String getBaseName() {
        return baseName;
    }

    public void resetTo(SegmentWriter writer) {
        lock.lock();
        try {
            writers.clear();
            sealedSegmentNames.clear();
            writers.add(writer);
            current = writer;
            synchronized (this) {
                sinSum = 0.0;
                cosSum = 0.0;
            }
            String name = writer.getSegmentName();
            int idx = 0;
            try {
                int cut1 = baseName.length() + 1;
                idx = Integer.parseInt(name.substring(cut1, name.length() - 8));
            } catch (Exception e) {
                System.err.println("WARN segment index parse: " + name + ": " + e.getMessage());
            }
            seq.set(idx + 1);
        } finally {
            lock.unlock();
        }
    }

    public double totalFillRatio() {
        int sealedCount = sealedSegmentNames.size();
        double writerSum = writers.stream().mapToDouble(SegmentWriter::getFillRatio).sum();
        int total = sealedCount + writers.size();
        return total > 0 ? (sealedCount + writerSum) / total : 1.0;
    }

    public boolean shouldCompact() {
        int totalCount = sealedSegmentNames.size() + writers.size();
        return totalCount > 3 && totalFillRatio() < 0.35;
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
        sealedSegmentNames.remove(writer.getSegmentName());
    }

    public synchronized void updatePhaseStats(double newPhase) {
        this.sinSum += Math.sin(newPhase);
        this.cosSum += Math.cos(newPhase);
    }

    public synchronized void removePhaseStats(double oldPhase) {
        this.sinSum -= Math.sin(oldPhase);
        this.cosSum -= Math.cos(oldPhase);
    }

    public synchronized double getAvgPhase() {
        if (!hasPhaseStatsInternal()) return 0.0;
        return Math.atan2(sinSum, cosSum);
    }

    public synchronized boolean hasPhaseStats() {
        return hasPhaseStatsInternal();
    }

    public synchronized OptionalDouble getAvgPhaseIfPresent() {
        if (!hasPhaseStatsInternal()) return OptionalDouble.empty();
        return OptionalDouble.of(Math.atan2(sinSum, cosSum));
    }

    private boolean hasPhaseStatsInternal() {
        return Math.abs(sinSum) > PHASE_STATS_EPSILON || Math.abs(cosSum) > PHASE_STATS_EPSILON;
    }

    public boolean containsSegment(String segmentName) {
        if (sealedSegmentNames.contains(segmentName)) return true;
        return getAll().stream()
                .anyMatch(w -> w.getSegmentName().equals(segmentName));
    }

    public void addSealedSegment(String segmentName) {
        lock.lock();
        try {
            if (!containsSegment(segmentName)) {
                sealedSegmentNames.add(segmentName);
            }
        } finally {
            lock.unlock();
        }
    }

    public List<String> getSealedSegmentNames() {
        return List.copyOf(sealedSegmentNames);
    }
}
