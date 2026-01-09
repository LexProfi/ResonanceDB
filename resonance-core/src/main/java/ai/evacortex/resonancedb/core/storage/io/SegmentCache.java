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

    private final ConcurrentMap<String, Long> versions;
    private final LoadingCache<Key, CachedReader> cache;
    private record Key(String name, long ver) {}

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public SegmentCache(Path dir) {
        this.versions = new ConcurrentHashMap<>();
        this.cache = Caffeine.newBuilder()
                .maximumWeight(Runtime.getRuntime().maxMemory() - (64L << 20))
                .weigher((Key k, CachedReader r) -> (int)Math.min(r.getWeightInBytes(), Integer.MAX_VALUE))
                .removalListener((Key k, CachedReader r, RemovalCause c) -> { if (r != null) r.close(); })
                .build(k -> CachedReader.open(dir.resolve(k.name)));
    }

    public void updateVersion(String seg, long lastOffset) {

        if (isClosed.get()) return;
        Long prev = versions.put(seg, lastOffset);
        if (prev != null && prev != lastOffset) {
            cache.invalidate(new Key(seg, prev));
        }
        cache.refresh(new Key(seg, lastOffset));
    }


    public CachedReader get(String seg) {
        if (isClosed.get()) return null;
        long v = versions.getOrDefault(seg, -1L);
        if (v < 0) return null;
        Key key = new Key(seg, v);
        return cache.getIfPresent(key);
    }

    @Override
    public void close() {
        isClosed.set(true);
        cache.invalidateAll();
        cache.cleanUp();
    }
}