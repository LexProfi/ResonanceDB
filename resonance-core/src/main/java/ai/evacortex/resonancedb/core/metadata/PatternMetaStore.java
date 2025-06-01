/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * PatternMetaStore is responsible for storing and retrieving
 * metadata associated with WavePattern entries.
 *
 * Metadata is stored as a JSON map: id → {key → value}.
 * The store is thread-safe and supports atomic persistence to disk.
 */
public class PatternMetaStore {

    private final Path metaFile;
    private final Map<String, Map<String, String>> store;
    private final ObjectMapper mapper;
    private final ReentrantReadWriteLock rwLock;

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
                TypeReference<Map<String, Map<String, String>>> typeRef = new TypeReference<>() {};
                Map<String, Map<String, String>> loaded = metaStore.mapper.readValue(in, typeRef);
                metaStore.store.putAll(loaded);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load metadata store", e);
            }
        }
        return metaStore;
    }

    private PatternMetaStore(Path metaFile) {
        this.metaFile = metaFile;
        this.store = new ConcurrentHashMap<>();
        this.mapper = new ObjectMapper();
        this.rwLock = new ReentrantReadWriteLock();
    }

    /**
     * Adds or updates metadata for a given ID.
     * Persists the store after modification.
     *
     * @param id       the WavePattern ID
     * @param metadata the metadata map
     */
    public void put(String id, Map<String, String> metadata) {
        rwLock.writeLock().lock();
        try {
            store.put(id, new HashMap<>(metadata));
            flush();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Removes metadata for a given ID and persists the change.
     *
     * @param id the pattern ID to remove
     */
    public void remove(String id) {
        rwLock.writeLock().lock();
        try {
            store.remove(id);
            flush();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Retrieves metadata for a given ID.
     *
     * @param id the pattern ID
     * @return metadata map or null if not present
     */
    public Map<String, String> get(String id) {
        rwLock.readLock().lock();
        try {
            return store.get(id);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Checks if metadata exists for the given ID.
     *
     * @param id pattern ID
     * @return true if present
     */
    public boolean contains(String id) {
        return store.containsKey(id);
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
     * Returns the underlying metadata map (read-only copy).
     */
    public Map<String, Map<String, String>> snapshot() {
        rwLock.readLock().lock();
        try {
            return new HashMap<>(store);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns all IDs currently stored.
     *
     * @return set of all pattern IDs
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