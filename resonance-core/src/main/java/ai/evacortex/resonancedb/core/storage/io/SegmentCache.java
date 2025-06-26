/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.io;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SegmentCache implements Closeable {

    /* 1.1  В памяти храним «текущую версию» (lastOffset) для каждого сегмента */
    private final ConcurrentMap<String, Long> versions = new ConcurrentHashMap<>();

    /* 1.2  Ключ = (segmentName, version) ― так Caffeine отличает старый mmap от нового */
    private record Key(String name, long ver) {}
    private final LoadingCache<Key, CachedReader> cache;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public SegmentCache(Path dir) {
        this.cache = Caffeine.newBuilder()
                .maximumWeight(Runtime.getRuntime().maxMemory() - (64L << 20))
                .weigher((Key k, CachedReader r) -> (int)Math.min(r.getWeightInBytes(), Integer.MAX_VALUE))
                .removalListener((Key k, CachedReader r, RemovalCause c) -> { if (r != null) r.close(); })
                .build(k -> CachedReader.open(dir.resolve(k.name)));
    }

    /* 1.3  Вызывает только писатель – фиксируем новую «версию» */
    public void updateVersion(String seg, long lastOffset) {
        //System.out.println("[CACHE] updateVersion: " + seg + " → " + lastOffset);
        if (isClosed.get()) return;
        Long prev = versions.put(seg, lastOffset);
        if (prev != null && prev != lastOffset) {
            cache.invalidate(new Key(seg, prev));
        }
        cache.refresh(new Key(seg, lastOffset));
    }

    /* 1.4  Быстрый доступ для любых query – без дисковых обращений */
    public CachedReader get(String seg) {
        //System.out.println("[CACHE] get: " + seg + " @ " + versions.get(seg));
        if (isClosed.get()) return null;
        long v = versions.getOrDefault(seg, -1L);
        if (v < 0) return null;
        Key key = new Key(seg, v);
        CachedReader reader = cache.getIfPresent(key);
        if (reader == null) return null;
        try {
            reader.ensureOpen(); // проверка живости
            return reader;
        } catch (IllegalStateException e) {
            cache.invalidate(key); // сбрасываем мёртвый reader
            return null;
        }
    }


    @Override
    public void close() {
        isClosed.set(true);
        cache.invalidateAll();
        cache.cleanUp();
    }
}