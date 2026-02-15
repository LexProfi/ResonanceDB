/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;

import java.io.*;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class ManifestIndex implements Closeable {

    private final Path indexFile;
    private final Map<String, PatternLocation> map;
    private final Set<String> knownSegments;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private ManifestIndex(Path indexFile) {
        this.indexFile = indexFile;
        this.map = new ConcurrentHashMap<>();
        this.knownSegments = ConcurrentHashMap.newKeySet();
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
                throw new RuntimeException("Failed to load manifest index: " + path, e);
            }
        }
        return idx;
    }

    public void add(String id, String segment, long offset, double phaseCenter) {
        lock.writeLock().lock();
        try {
            map.put(id, new PatternLocation(segment, offset, phaseCenter));
            knownSegments.add(segment);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void registerSegmentIfAbsent(String segmentName) {
        lock.writeLock().lock();
        try {
            knownSegments.add(segmentName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(String id) {
        lock.writeLock().lock();
        try {
            if (!map.containsKey(id)) throw new PatternNotFoundException(id);
            map.remove(id);
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
        return map.containsKey(id);
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
        lock.writeLock().lock();
        try {
            persistToFile();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        flush();
    }

    private void persistToFile() {
        lock.readLock().lock();
        try {
            Files.createDirectories(indexFile.getParent());
            Path tmp = indexFile.resolveSibling(indexFile.getFileName().toString() + ".tmp");
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmp)))) {
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
            }

            try {
                Files.move(tmp, indexFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, indexFile, StandardCopyOption.REPLACE_EXISTING);
            }

        } catch (IOException e) {
            System.err.printf("Manifest flush failed: %s%n", e);
            throw new RuntimeException("Failed to write manifest index", e);
        } finally {
            lock.readLock().unlock();
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
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replace(String oldId, String newId,
                        String newSegment, long newOffset, double newPhaseCenter) {
        this.remove(oldId);
        this.add(newId, newSegment, newOffset, newPhaseCenter);
    }

    public record PatternLocation(String segmentName, long offset, double phaseCenter) {}
}
