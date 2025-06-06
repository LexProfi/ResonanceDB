/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.time.Duration;
import java.util.concurrent.*;

public class ManifestIndex implements Closeable {

    private final Path indexFile;
    private final Map<String, PatternLocation> map;
    private final Set<String> knownSegments;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ScheduledExecutorService scheduler;
    private volatile boolean dirty = false;

    private ManifestIndex(Path indexFile) {
        this.indexFile = indexFile;
        this.map = new ConcurrentHashMap<>();
        this.knownSegments = ConcurrentHashMap.newKeySet();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("manifest-autoflush");
            return t;
        });
    }

    public static ManifestIndex loadOrCreate(Path path) {
        ManifestIndex idx = new ManifestIndex(path);
        if (Files.exists(path)) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
                int segmentCount = in.readInt();
                for (int i = 0; i < segmentCount; i++) {
                    idx.knownSegments.add(in.readUTF());
                }
                int patternCount = in.readInt();
                for (int i = 0; i < patternCount; i++) {
                    String id = in.readUTF();
                    String segment = in.readUTF();
                    long offset = in.readLong();
                    double phaseCenter;
                    try {
                        phaseCenter = in.readDouble();
                    } catch (EOFException e) {
                        phaseCenter = 0.0;
                    }
                    idx.map.put(id, new PatternLocation(segment, offset, phaseCenter));
                    idx.knownSegments.add(segment);
                }
            } catch (IOException e) {
                Path backup = path.resolveSibling(path.getFileName().toString() + ".bak");
                if (Files.exists(backup)) {
                    return loadOrCreate(backup);
                } else {
                    throw new RuntimeException("Failed to load manifest index", e);
                }
            }
        }
        return idx;
    }

    public void startAutoFlush(Duration interval) {
        scheduler.scheduleAtFixedRate(() -> {
            if (dirty) flush();
        }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void add(String id, String segment, long offset, double phaseCenter) {
        lock.writeLock().lock();
        try {
            map.put(id, new PatternLocation(segment, offset, phaseCenter));
            knownSegments.add(segment);
            dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void registerSegmentIfAbsent(String segmentName) {
        lock.writeLock().lock();
        try {
            if (knownSegments.add(segmentName)) {
                dirty = true;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(String id) {
        lock.writeLock().lock();
        try {
            if (!map.containsKey(id)) throw new PatternNotFoundException(id);
            map.remove(id);
            dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public PatternLocation get(String id) {
        lock.readLock().lock();
        try {
            return map.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean contains(String id) {
        lock.readLock().lock();
        try {
            return map.containsKey(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<String> getAllSegmentNames() {
        lock.readLock().lock();
        try {
            Set<String> result = new HashSet<>(knownSegments);
            for (PatternLocation loc : map.values()) {
                result.add(loc.segmentName());
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<PatternLocation> getAllLocations() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(map.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    public void flush() {
        lock.readLock().lock();
        try {
            persistToFile();
            dirty = false;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        flush();
    }

    private void persistToFile() {
        try {
            Files.createDirectories(indexFile.getParent());

            if (Files.exists(indexFile)) {
                Path backup = indexFile.resolveSibling(indexFile.getFileName().toString() + ".bak");
                Files.copy(indexFile, backup, StandardCopyOption.REPLACE_EXISTING);
            }

            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(indexFile)))) {
                lock.readLock().lock();
                try {
                    out.writeInt(knownSegments.size());
                    for (String seg : knownSegments) {
                        out.writeUTF(seg);
                    }

                    out.writeInt(map.size());
                    for (Map.Entry<String, PatternLocation> e : map.entrySet()) {
                        out.writeUTF(e.getKey());
                        PatternLocation loc = e.getValue();
                        out.writeUTF(loc.segmentName());
                        out.writeLong(loc.offset());
                        out.writeDouble(loc.phaseCenter());
                    }
                } finally {
                    lock.readLock().unlock();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write manifest index", e);
        }
    }

    public void ensureFileExists() {
        try {
            Files.createDirectories(indexFile.getParent());
            if (!Files.exists(indexFile)) {
                Files.createFile(indexFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to ensure manifest index file exists", e);
        }
    }

    public void replace(String id,
                        String oldSegment,
                        long oldOffset,
                        String newSegment,
                        long newOffset,
                        double newPhaseCenter) {
        lock.writeLock().lock();
        try {
            PatternLocation current = map.get(id);
            if (current == null ||
                    !current.segmentName().equals(oldSegment) ||
                    current.offset() != oldOffset) {
                throw new IllegalStateException("Attempt to replace non-matching entry: " + id);
            }

            map.put(id, new PatternLocation(newSegment, newOffset, newPhaseCenter));
            dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public record PatternLocation(String segmentName, long offset, double phaseCenter) {}
}