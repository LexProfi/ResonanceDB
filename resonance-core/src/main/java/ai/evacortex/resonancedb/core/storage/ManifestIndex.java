/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
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

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.concurrent.*;

public class ManifestIndex implements Closeable {

    private final Path indexFile;
    private final Map<String, PatternLocation> map;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ScheduledExecutorService scheduler;
    private volatile boolean dirty = false;

    private ManifestIndex(Path indexFile) {
        this.indexFile = indexFile;
        this.map = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("manifest-autoflush");
            return t;
        });
    }

    /**
     * Loads an existing manifest index or creates a new one if absent.
     */
    public static ManifestIndex loadOrCreate(Path path) {
        ManifestIndex idx = new ManifestIndex(path);
        if (Files.exists(path)) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    String id = in.readUTF();
                    String segment = in.readUTF();
                    long offset = in.readLong();
                    double phaseCenter;
                    try {
                        phaseCenter = in.readDouble();
                    } catch (EOFException e) {
                        phaseCenter = 0.0; // legacy compatibility
                    }
                    idx.map.put(id, new PatternLocation(segment, offset, phaseCenter));
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

    /**
     * Starts automatic periodic flushing.
     */
    public void startAutoFlush(Duration interval) {
        scheduler.scheduleAtFixedRate(() -> {
            if (dirty) flush();
        }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Registers a new pattern ID and its location.
     */
    public void add(String id, String segment, long offset, double phaseCenter) {
        lock.writeLock().lock();
        try {
            map.put(id, new PatternLocation(segment, offset, phaseCenter));
            dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a pattern ID from the index.
     */
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

    /**
     * Returns the stored location of a given ID.
     */
    public PatternLocation get(String id) {
        lock.readLock().lock();
        try {
            return map.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns whether a given ID is indexed.
     */
    public boolean contains(String id) {
        lock.readLock().lock();
        try {
            return map.containsKey(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns all unique segment names used in this index.
     */
    public Set<String> getAllSegmentNames() {
        lock.readLock().lock();
        try {
            Set<String> result = new HashSet<>();
            for (PatternLocation loc : map.values()) result.add(loc.segmentName());
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns all known locations for external access (e.g. PhaseShardSelector).
     */
    public Collection<PatternLocation> allLocations() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(map.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Flushes the index to disk with backup support.
     */
    public void flush() {
        lock.readLock().lock();
        try {
            persistToFile();
            dirty = false;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gracefully shuts down the flush scheduler and persists data.
     */
    @Override
    public void close() {
        scheduler.shutdownNow();
        flush();
    }

    private void persistToFile() {
        try {
            if (Files.exists(indexFile)) {
                Path backup = indexFile.resolveSibling(indexFile.getFileName().toString() + ".bak");
                Files.copy(indexFile, backup, StandardCopyOption.REPLACE_EXISTING);
            }

            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(indexFile)))) {
                out.writeInt(map.size());
                for (Map.Entry<String, PatternLocation> e : map.entrySet()) {
                    out.writeUTF(e.getKey());
                    PatternLocation loc = e.getValue();
                    out.writeUTF(loc.segmentName());
                    out.writeLong(loc.offset());
                    out.writeDouble(loc.phaseCenter());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write manifest index", e);
        }
    }

    /**
     * Returns a snapshot of all stored PatternLocations.
     */
    public List<PatternLocation> getAllLocations() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(map.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Describes physical segment, offset, and phase for pattern.
     */
    public record PatternLocation(String segmentName, long offset, double phaseCenter) {}
}