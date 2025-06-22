package ai.evacortex.resonancedb.core.storage.io;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.nio.file.Path;

public class SegmentReaderCache {
    private final LoadingCache<String, CachedReader> cache;

    public SegmentReaderCache(Path segmentDir, int maxEntries) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxEntries)
                .build(name -> CachedReader.open(segmentDir.resolve(name)));  // ⟵ УБРАНО ".segment"
    }

    public CachedReader get(String segmentName) {
        return cache.get(segmentName);
    }

    public void invalidate(String segmentName) {
        cache.invalidate(segmentName);
    }

    /** Безопасная инвалидация кэша, если сегмент существует */
    public void invalidateIfCached(String segmentName) {
        if (cache.asMap().containsKey(segmentName)) {
            cache.invalidate(segmentName);
        }
    }

    public void close() {
        cache.asMap().values().forEach(CachedReader::close);
    }
}