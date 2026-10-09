/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
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
import java.util.concurrent.atomic.AtomicLong;

public class SegmentCache implements Closeable {

    private final ConcurrentMap<String, Long> versions;
    private final LoadingCache<Key, CachedReader> cache;
    private record Key(String name, long ver) {}

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final AtomicLong loadCount = new AtomicLong();
    private final AtomicLong evictionCount = new AtomicLong();

    public SegmentCache(Path dir) {
        this(dir, Long.getLong("resonance.cache.maxBytes",
                Runtime.getRuntime().maxMemory() - (64L << 20)));
    }

    public SegmentCache(Path dir, long maxWeightBytes) {
        this.versions = new ConcurrentHashMap<>();
        this.cache = Caffeine.newBuilder()
                .maximumWeight(maxWeightBytes)
                .weigher((Key k, CachedReader r) -> (int) Math.min(r.getWeightInBytes(), Integer.MAX_VALUE))
                .removalListener((Key k, CachedReader r, RemovalCause c) -> {
                    evictionCount.incrementAndGet();
                    if (r != null) r.close();
                })
                .build(k -> {
                    loadCount.incrementAndGet();
                    return CachedReader.open(dir.resolve(k.name));
                });
    }

    public void updateVersion(String seg, long lastOffset) {
        if (isClosed.get()) return;
        Long prev = versions.put(seg, lastOffset);
        if (prev != null && prev != lastOffset) {
            cache.invalidate(new Key(seg, prev));
        }
        Key key = new Key(seg, lastOffset);
        cache.invalidate(key);
        try {
            cache.get(key);
        } catch (Exception ignored) {
        }
    }

    public CachedReader getOrLoad(String seg) {
        if (isClosed.get()) return null;
        long v = versions.getOrDefault(seg, -1L);
        if (v < 0) return null;
        Key key = new Key(seg, v);
        try {
            return cache.get(key);
        } catch (Exception e) {
            return null;
        }
    }

    public long getLoadCount() { return loadCount.get(); }
    public long getEvictionCount() { return evictionCount.get(); }
    public long estimatedSize() { return cache.estimatedSize(); }

    @Override
    public void close() {
        isClosed.set(true);
        cache.invalidateAll();
        cache.cleanUp();
    }
}
