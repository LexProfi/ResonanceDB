/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.storage.io.SegmentCache;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlushDispatcher implements Closeable {

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentMap<String, FlushTask> tasks = new ConcurrentHashMap<>();
    private final AtomicBoolean flushing = new AtomicBoolean(false);

    public record FlushTask(String segmentName, SegmentWriter writer, SegmentCache readerCache) implements Runnable {
        @Override
        public void run() {
            try {
                long newVer = writer.flush();
                writer.sync();
                readerCache.updateVersion(segmentName, newVer);
            } catch (Exception e) {
                System.err.println("Flush failed for segment " + segmentName + ": " + e.getMessage());
            }
        }
    }

    public FlushDispatcher(Duration interval) {
        scheduler.scheduleAtFixedRate(this::flushNext, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void register(FlushTask task) {
        tasks.put(task.segmentName(), task);
    }

    public void flushNow() {
        Map<String, FlushTask> toFlush = new HashMap<>(tasks);
        tasks.keySet().removeAll(toFlush.keySet());
        toFlush.values().forEach(Runnable::run);
    }

    private void flushNext() {
        if (!flushing.compareAndSet(false, true)) return;
        try {
            flushNow();
        } finally {
            flushing.set(false);
        }
    }

    @Override
    public void close() {
        flushNow();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) scheduler.shutdownNow();
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}