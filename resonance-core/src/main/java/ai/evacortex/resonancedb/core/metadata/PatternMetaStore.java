/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
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

public class PatternMetaStore {

    private final Path metaFile;
    private final Map<String, PatternMeta> store;
    private final ObjectMapper mapper;
    private final ReentrantReadWriteLock rwLock;

    public record PatternMeta(Map<String, String> metadata) {}

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

    public void put(String hashId, Map<String, String> metadata) {
        rwLock.writeLock().lock();
        try {
            store.put(hashId, new PatternMeta(new HashMap<>(metadata)));
            flush();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void remove(String hashId) {
        rwLock.writeLock().lock();
        try {
            store.remove(hashId);
            flush();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Map<String, String> getMetadata(String hashId) {
        rwLock.readLock().lock();
        try {
            PatternMeta meta = store.get(hashId);
            return meta != null ? new HashMap<>(meta.metadata()) : null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public boolean contains(String hashId) {
        rwLock.readLock().lock();
        try {
            return store.containsKey(hashId);
        } finally {
            rwLock.readLock().unlock();
        }
    }

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

    public Set<String> getAllIds() {
        rwLock.readLock().lock();
        try {
            return new HashSet<>(store.keySet());
        } finally {
            rwLock.readLock().unlock();
        }
    }
}
