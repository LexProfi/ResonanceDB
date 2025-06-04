/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * PatternMetaStore is responsible for storing and retrieving
 * metadata associated with WavePattern entries.

 * Metadata is stored as a JSON map: id → {key → value}.
 * The store is thread-safe and supports atomic persistence to disk.
 */
public class PatternMetaStore {

    private final Path metaFile;
    private final Map<String, PatternMeta> store;
    private final ObjectMapper mapper;
    private final ReentrantReadWriteLock rwLock;

    public record PatternMeta(Map<String, String> metadata) {}

    /**
     * Loads an existing metadata file or creates a new empty store.
     *
     * @param path path to pattern-meta.json
     * @return initialized PatternMetaStore
     */
    public static PatternMetaStore loadOrCreate(Path path) {
        PatternMetaStore metaStore = new PatternMetaStore(path);
        if (Files.exists(path)) {
            try (InputStream in = Files.newInputStream(path)) {
                TypeReference<Map<String, PatternMeta>> typeRef = new TypeReference<>() {};
                Map<String, PatternMeta> loaded = metaStore.mapper.readValue(in, typeRef);
                synchronized (metaStore.store) {
                    metaStore.store.putAll(loaded);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load metadata store", e);
            }
        }
        return metaStore;
    }

    private PatternMetaStore(Path metaFile) {
        this.metaFile = metaFile;
        this.mapper = new ObjectMapper();
        this.store = Collections.synchronizedMap(new HashMap<>());
        this.rwLock = new ReentrantReadWriteLock();
    }

    /**
     * Adds or updates metadata with explicit rawId. Persists the store after modification.
     *
     * @param hashId SHA-256 ID of the pattern
     * @param metadata arbitrary metadata map
     */
    public void put(String hashId, Map<String, String> metadata) {
        rwLock.writeLock().lock();
        try {
            store.put(hashId, new PatternMeta(new HashMap<>(metadata)));
            flush();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Removes metadata for a given hash ID and persists the change.
     *
     * @param hashId SHA-256 ID of the pattern
     */
    public void remove(String hashId) {
        rwLock.writeLock().lock();
        try {
            store.remove(hashId);
            flush();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Retrieves metadata for a given hash ID.
     *
     * @param hashId SHA-256 ID of the pattern
     * @return metadata map or null if not present
     */
    public Map<String, String> getMetadata(String hashId) {
        rwLock.readLock().lock();
        try {
            PatternMeta meta = store.get(hashId);
            return meta != null ? new HashMap<>(meta.metadata()) : null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Checks if metadata exists for the given hash ID.
     *
     * @param hashId SHA-256 ID of the pattern
     * @return true if present
     */
    public boolean contains(String hashId) {
        rwLock.readLock().lock();
        try {
            return store.containsKey(hashId);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Persists the metadata store to disk as JSON.
     */
    public void flush() {
        rwLock.writeLock().lock();
        try {
            Files.createDirectories(metaFile.getParent());
            try (OutputStream out = Files.newOutputStream(metaFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(out, store);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush metadata store", e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Returns a snapshot of the entire metadata map.
     */
    public Map<String, PatternMeta> snapshot() {
        rwLock.readLock().lock();
        try {
            Map<String, PatternMeta> copy = new HashMap<>();
            for (var entry : store.entrySet()) {
                copy.put(entry.getKey(), new PatternMeta(new HashMap<>(entry.getValue().metadata())));
            }
            return copy;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns all hash IDs currently stored.
     *
     * @return set of all pattern ID hashes
     */
    public Set<String> getAllIds() {
        rwLock.readLock().lock();
        try {
            return new HashSet<>(store.keySet());
        } finally {
            rwLock.readLock().unlock();
        }
    }
}
