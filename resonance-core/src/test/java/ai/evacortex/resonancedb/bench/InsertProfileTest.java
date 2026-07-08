/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.bench;

import ai.evacortex.resonancedb.core.storage.StoreRuntimeServices;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

/**
 * Micro-profiler: isolates per-component cost on the insert path.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("benchmark")
class InsertProfileTest {

    private static final int DIM = 1536;

    @TempDir
    Path tempDir;

    private StoreRuntimeServices runtime;

    @BeforeAll
    void init() {
        runtime = StoreRuntimeServices.fromSystemProperties();
    }

    @AfterAll
    void close() {
        if (runtime != null) runtime.close();
    }

    @Test
    @DisplayName("Profile: computeContentHash throughput")
    void profileHash() {
        Random rng = new Random(42);
        int N = 10_000;
        WavePattern[] patterns = new WavePattern[N];
        for (int i = 0; i < N; i++) {
            patterns[i] = randomPattern(rng, DIM);
        }

        // Warmup
        for (int i = 0; i < 500; i++) {
            HashingUtil.computeContentHash(patterns[i % N]);
        }

        long t0 = System.nanoTime();
        for (int i = 0; i < N; i++) {
            HashingUtil.computeContentHash(patterns[i]);
        }
        long elapsed = System.nanoTime() - t0;
        double perOp = elapsed / (double) N / 1_000_000.0;
        double opsPerSec = N / (elapsed / 1_000_000_000.0);

        System.out.printf("  computeContentHash: %.3f ms/op, %.0f ops/sec (N=%d, dim=%d)%n",
                perOp, opsPerSec, N, DIM);
    }

    @Test
    @DisplayName("Profile: batch insert at various N with checkpoints")
    void profileInsert() {
        System.setProperty("resonance.insert.batchSize", "100000");

        for (int n : new int[]{1_000, 5_000, 10_000, 25_000, 50_000}) {
            Path storeDir = tempDir.resolve("profile-batch-" + n);
            WavePatternStoreImpl store = new WavePatternStoreImpl(storeDir, DIM, runtime);
            Random rng = new Random(12345L);

            try {
                long start = System.nanoTime();
                long[] checkpoints = new long[5];
                int checkIdx = 0;
                int step = n / 5;

                for (int i = 0; i < n; i++) {
                    WavePattern p = randomPattern(rng, DIM);
                    try { store.insert(p, Map.of()); } catch (Exception e) { /* skip */ }

                    if (step > 0 && (i + 1) % step == 0 && checkIdx < 5) {
                        checkpoints[checkIdx++] = System.nanoTime();
                    }
                }

                long totalElapsed = System.nanoTime() - start;
                double totalMs = totalElapsed / 1_000_000.0;
                double avgPerSec = n / (totalMs / 1000.0);

                System.out.printf("  batch N=%d: total=%.0f ms, avg=%.0f inserts/sec%n",
                        n, totalMs, avgPerSec);

                for (int c = 0; c < checkIdx; c++) {
                    int count = (c + 1) * step;
                    long segElapsed = (c == 0)
                            ? checkpoints[c] - start
                            : checkpoints[c] - checkpoints[c - 1];
                    double segMs = segElapsed / 1_000_000.0;
                    double segPerSec = step / (segMs / 1000.0);
                    System.out.printf("    %d-%d: %.0f inserts/sec (%.0f ms for %d)%n",
                            (c == 0 ? 0 : c * step), count, segPerSec, segMs, step);
                }

            } finally {
                try { store.close(); } catch (Exception e) { /* ignore */ }
            }
        }

        System.setProperty("resonance.insert.batchSize", "1");
    }

    @Test
    @DisplayName("Profile: raw mmap write speed")
    void profileMmapWrite(@TempDir Path dir) throws Exception {
        int N = 2_000;  // ~49MB, fits in 64MB mmap
        int patternBytes = 4 + DIM * 8 * 2; // len + amp + phase
        long fileSize = 64L * 1024 * 1024; // 64MB

        java.nio.file.Path file = dir.resolve("mmap-bench.dat");
        java.nio.file.Files.createFile(file);
        try (var raf = new java.io.RandomAccessFile(file.toFile(), "rw");
             var chan = raf.getChannel()) {
            java.nio.MappedByteBuffer mmap = chan.map(
                    java.nio.channels.FileChannel.MapMode.READ_WRITE, 0, fileSize);
            mmap.order(java.nio.ByteOrder.LITTLE_ENDIAN);

            Random rng = new Random(42);
            WavePattern[] patterns = new WavePattern[N];
            for (int i = 0; i < N; i++) {
                patterns[i] = randomPattern(rng, DIM);
            }

            // Warmup
            int pos = 0;
            for (int i = 0; i < 100; i++) {
                WavePattern p = patterns[i % N];
                mmap.position(pos);
                mmap.putInt(DIM);
                for (double v : p.amplitude()) mmap.putDouble(v);
                for (double v : p.phase()) mmap.putDouble(v);
                pos += patternBytes;
            }

            // Measure
            pos = 0;
            mmap.position(0);
            long t0 = System.nanoTime();
            for (int i = 0; i < N; i++) {
                WavePattern p = patterns[i % N];
                mmap.position(pos);
                mmap.putInt(DIM);
                for (double v : p.amplitude()) mmap.putDouble(v);
                for (double v : p.phase()) mmap.putDouble(v);
                pos += patternBytes;
            }
            long elapsed = System.nanoTime() - t0;
            double msPerOp = elapsed / (double) N / 1_000_000.0;
            double opsPerSec = N / (elapsed / 1_000_000_000.0);
            System.out.printf("  raw mmap write (per-element): %.3f ms/op, %.0f ops/sec%n",
                    msPerOp, opsPerSec);

            // Now test bulk write via DoubleBuffer
            pos = 0;
            mmap.position(0);
            java.nio.ByteBuffer staging = java.nio.ByteBuffer.allocate(DIM * 8 * 2)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN);
            t0 = System.nanoTime();
            for (int i = 0; i < N; i++) {
                WavePattern p = patterns[i % N];
                staging.clear();
                staging.asDoubleBuffer().put(p.amplitude()).put(p.phase());
                staging.limit(DIM * 8 * 2);
                mmap.position(pos);
                mmap.putInt(DIM);
                mmap.put(staging);
                pos += patternBytes;
            }
            elapsed = System.nanoTime() - t0;
            msPerOp = elapsed / (double) N / 1_000_000.0;
            opsPerSec = N / (elapsed / 1_000_000_000.0);
            System.out.printf("  raw mmap write (bulk staging): %.3f ms/op, %.0f ops/sec%n",
                    msPerOp, opsPerSec);

            // Test pure hash speed
            t0 = System.nanoTime();
            for (int i = 0; i < N; i++) {
                HashingUtil.computeContentHash(patterns[i % N]);
            }
            elapsed = System.nanoTime() - t0;
            msPerOp = elapsed / (double) N / 1_000_000.0;
            opsPerSec = N / (elapsed / 1_000_000_000.0);
            System.out.printf("  computeContentHash: %.3f ms/op, %.0f ops/sec%n",
                    msPerOp, opsPerSec);
        }
    }

    private static WavePattern randomPattern(Random rng, int dim) {
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = rng.nextDouble();
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }
}
