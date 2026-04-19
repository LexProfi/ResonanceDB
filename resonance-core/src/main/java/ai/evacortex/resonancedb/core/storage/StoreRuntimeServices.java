/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceKernel;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public final class StoreRuntimeServices implements Closeable {

    private final ForkJoinPool queryPool;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ResonanceKernel resonanceKernel;
    private final AdaptiveIoGovernor ioGovernor;
    private final boolean ownResources;
    private final boolean flushAsync;
    private final Duration flushInterval;

    public StoreRuntimeServices(ForkJoinPool queryPool,
                                ScheduledThreadPoolExecutor scheduler,
                                ResonanceKernel resonanceKernel,
                                AdaptiveIoGovernor ioGovernor,
                                boolean flushAsync,
                                Duration flushInterval,
                                boolean ownResources) {
        this.queryPool = Objects.requireNonNull(queryPool, "queryPool must not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.resonanceKernel = Objects.requireNonNull(resonanceKernel, "resonanceKernel must not be null");
        this.ioGovernor = Objects.requireNonNull(ioGovernor, "ioGovernor must not be null");
        this.flushInterval = Objects.requireNonNull(flushInterval, "flushInterval must not be null");
        this.flushAsync = flushAsync;
        this.ownResources = ownResources;
    }

    public static StoreRuntimeServices fromSystemProperties() {
        Runtime rt = Runtime.getRuntime();
        long maxHeap = rt.maxMemory();
        int hwThreads = Math.max(1, rt.availableProcessors());

        int poolParallelism = computePoolParallelism(hwThreads, maxHeap);
        int schedulerThreads = Integer.getInteger(
                "resonance.runtime.scheduler.threads",
                Math.max(2, Math.min(4, Math.max(2, hwThreads / 4)))
        );

        boolean useNativeKernel = Boolean.parseBoolean(System.getProperty("resonance.kernel.native", "false"));
        boolean flushAsync = Boolean.parseBoolean(System.getProperty("resonance.flush.async", "false"));
        Duration flushInterval = Duration.ofMillis(
                Long.getLong("resonance.flush.interval.millis", 5L)
        );

        ForkJoinPool queryPool = new ForkJoinPool(
                poolParallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                false
        );

        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(
                schedulerThreads,
                namedDaemonFactory("resonancedb-runtime")
        );
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        ResonanceKernel kernel = createKernel(useNativeKernel);
        AdaptiveIoGovernor ioGovernor = new AdaptiveIoGovernor(scheduler, hwThreads, maxHeap);

        return new StoreRuntimeServices(
                queryPool,
                scheduler,
                kernel,
                ioGovernor,
                flushAsync,
                flushInterval,
                true
        );
    }

    public ForkJoinPool queryPool() {
        return queryPool;
    }

    public ScheduledThreadPoolExecutor scheduler() {
        return scheduler;
    }

    public ResonanceKernel resonanceKernel() {
        return resonanceKernel;
    }

    public AdaptiveIoGovernor ioGovernor() {
        return ioGovernor;
    }

    public boolean flushAsync() {
        return flushAsync;
    }

    public Duration flushInterval() {
        return flushInterval;
    }

    @Override
    public void close() {
        if (!ownResources) {
            return;
        }

        ioGovernor.close();

        queryPool.shutdown();
        try {
            if (!queryPool.awaitTermination(30, TimeUnit.SECONDS)) {
                queryPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            queryPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static ResonanceKernel createKernel(boolean useNativeKernel) {
        if (!useNativeKernel) {
            return new JavaKernel();
        }

        try {
            Class<?> clazz = Class.forName("ai.evacortex.resonancedb.core.engine.NativeKernel");
            if (!ResonanceKernel.class.isAssignableFrom(clazz)) {
                return new JavaKernel();
            }
            Constructor<?> ctor = clazz.getDeclaredConstructor();
            ctor.setAccessible(true);
            return (ResonanceKernel) ctor.newInstance();
        } catch (Throwable ignored) {
            return new JavaKernel();
        }
    }

    private static int computePoolParallelism(int hwThreads, long maxHeap) {
        int base = hwThreads;
        if (maxHeap < (768L << 20)) {
            base = Math.max(1, (int) Math.ceil(hwThreads * 0.75));
        } else if (maxHeap < (1536L << 20)) {
            base = Math.max(1, (int) Math.ceil(hwThreads * 0.9));
        }
        return Math.min(Math.max(1, base), hwThreads * 4);
    }

    private static ThreadFactory namedDaemonFactory(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * Shared adaptive I/O governor for all open corpora.
     *
     * <p>This preserves the spirit of the original adaptive batch permit controller,
     * but moves it to the process level so parallel queries across multiple corpora
     * do not silently multiply the effective I/O concurrency.</p>
     */
    public static final class AdaptiveIoGovernor implements Closeable {

        private final Semaphore limiter;
        private final int ioPermitsMaxBound;
        private final AtomicInteger ioPermitsCurrent = new AtomicInteger(1);
        private final LongAdder ioAcquireCount = new LongAdder();
        private final LongAdder ioWaitNanos = new LongAdder();
        private final long maxHeap;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ScheduledFuture<?> rebalanceTask;
        private volatile long lastRebalanceNanos = System.nanoTime();

        AdaptiveIoGovernor(ScheduledExecutorService scheduler, int hwThreads, long maxHeap) {
            this.maxHeap = maxHeap;

            int ioBase = Math.max(1, hwThreads / 2);
            if (maxHeap < (768L << 20)) {
                ioBase = Math.max(1, hwThreads / 3);
            }

            this.ioPermitsMaxBound = Math.max(1, ioBase);

            int startPermits = Math.max(
                    1,
                    Math.min(ioPermitsMaxBound, Math.max(1, hwThreads / 3))
            );

            this.limiter = new Semaphore(startPermits, true);
            this.ioPermitsCurrent.set(startPermits);

            this.rebalanceTask = scheduler.scheduleAtFixedRate(
                    this::rebalanceScheduled,
                    2, 2, TimeUnit.SECONDS
            );
        }

        public void acquire() {
            if (closed.get()) {
                return;
            }

            long start = System.nanoTime();
            try {
                limiter.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                long waited = System.nanoTime() - start;
                ioAcquireCount.increment();
                ioWaitNanos.add(waited);
                maybeRebalanceHotpath();
            }
        }

        public void release() {
            if (!closed.get()) {
                limiter.release();
            }
        }

        public int currentPermits() {
            return ioPermitsCurrent.get();
        }

        public int maxPermits() {
            return ioPermitsMaxBound;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            rebalanceTask.cancel(false);
        }

        private void maybeRebalanceHotpath() {
            long cnt = ioAcquireCount.sum();
            if ((cnt & 0x7FFF) != 0) {
                return;
            }

            long now = System.nanoTime();
            if (now - lastRebalanceNanos < 200_000_000L) {
                return;
            }

            lastRebalanceNanos = now;
            rebalanceInternal(false);
        }

        private void rebalanceScheduled() {
            rebalanceInternal(true);
        }

        private void rebalanceInternal(boolean scheduled) {
            if (closed.get()) {
                return;
            }

            final long count = scheduled ? ioAcquireCount.sumThenReset() : ioAcquireCount.sum();
            final long wait = scheduled ? ioWaitNanos.sumThenReset() : ioWaitNanos.sum();

            if (count == 0L) {
                return;
            }

            double avgWaitMicros = (wait / (double) count) / 1000.0;

            Runtime rt = Runtime.getRuntime();
            long used = rt.totalMemory() - rt.freeMemory();
            long headroom = rt.maxMemory() - used;
            double headroomRatio = Math.max(0.0, headroom / (double) maxHeap);

            int curr = ioPermitsCurrent.get();

            if (headroomRatio < 0.10 && curr > 1) {
                if (limiter.tryAcquire()) {
                    ioPermitsCurrent.decrementAndGet();
                }
                return;
            }

            if (avgWaitMicros > 200.0 && curr < ioPermitsMaxBound) {
                limiter.release();
                ioPermitsCurrent.incrementAndGet();
                return;
            }

            if (avgWaitMicros < 10.0 && headroomRatio < 0.25 && curr > 1) {
                if (limiter.tryAcquire()) {
                    ioPermitsCurrent.decrementAndGet();
                }
            }
        }
    }
}