/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceEngine;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

//@Disabled("Smoke benchmark — run manually from IDE")
public class WavePatternLoadTest {

    private static final int PATTERN_COUNT = Integer.getInteger("patterns", 50_000);
    private static final int PATTERN_LEN = Integer.getInteger("length", 2048);
    private static final int QUERY_REPS = Integer.getInteger("queries", 100);
    private static final Path DB_ROOT = Paths.get(System.getProperty("resonance.test.dir", "C:\\Users\\Aleksandr\\Downloads\\64x2048x50k\\resdb-load"));

    private static final int[] TOP_K_BUCKETS = Arrays.stream(System.getProperty("topKs", "1,10,100").split(","))
            .mapToInt(Integer::parseInt).toArray();

    private static WavePattern randomPattern(int len, ThreadLocalRandom rnd) {
        double[] amp = new double[len];
        double[] phase = new double[len];
        for (int i = 0; i < len; i++) {
            amp[i] = rnd.nextDouble();
            phase[i] = rnd.nextDouble() * Math.PI * 2 - Math.PI;
        }
        return new WavePattern(amp, phase);
    }

    private static double percentile(List<Long> nanos, double p) {
        if (nanos.isEmpty()) return 0;
        List<Long> sorted = new ArrayList<>(nanos);
        sorted.sort(Long::compareTo);
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1))) / 1e6;
    }

    private static void warmUpSSD(Path segmentsDir) throws IOException {
        if (!Files.isDirectory(segmentsDir)) return;
        byte[] buf = new byte[8192];
        for (Path p : Files.walk(segmentsDir).filter(Files::isRegularFile).toList()) {
            try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
                ByteBuffer bb = ByteBuffer.wrap(buf);
                while (ch.read(bb) != -1) bb.clear();
            }
        }
    }

    private static void showSystemInfo() {
        System.out.printf("Started at: %s%n", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.printf("OS   : %s %s%n", System.getProperty("os.name"), System.getProperty("os.version"));
        System.out.printf("CPU  : %d logical cores%n", Runtime.getRuntime().availableProcessors());
        System.out.printf("JVM  : %s %s%n", System.getProperty("java.vm.name"), System.getProperty("java.version"));
        long maxMemory = Runtime.getRuntime().maxMemory();
        long totalMemoryMB = maxMemory / (1024 * 1024);
        System.out.printf("RAM  : ~%d MB (max JVM heap)%n", totalMemoryMB);
    }

    @Test
    public void generateAndInsertPatterns() throws Exception {
        Files.createDirectories(DB_ROOT);
        System.out.printf("=== GENERATE %,d x L=%s patterns to %s ===%n", PATTERN_COUNT, PATTERN_LEN, DB_ROOT);
        showSystemInfo();
        ResonanceEngine.setBackend(new JavaKernel());

        try (WavePatternStoreImpl store = new WavePatternStoreImpl(DB_ROOT)) {
            int threads = Runtime.getRuntime().availableProcessors();
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(PATTERN_COUNT);

            ConcurrentMap<Integer, List<Long>> bucketTimes = new ConcurrentHashMap<>();
            List<Long> allTimes = Collections.synchronizedList(new ArrayList<>());

            int bucketSize = Math.max(PATTERN_COUNT / 10, 1);

            for (int i = 0; i < PATTERN_COUNT; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        WavePattern wp = randomPattern(PATTERN_LEN, ThreadLocalRandom.current());
                        long t0 = System.nanoTime();
                        store.insert(wp, Collections.emptyMap());
                        long dt = System.nanoTime() - t0;

                        allTimes.add(dt);
                        int bucket = Math.min(index / bucketSize, 9);
                        bucketTimes.computeIfAbsent(bucket, k -> Collections.synchronizedList(new ArrayList<>())).add(dt);

                        if ((index + 1) % bucketSize == 0) {
                            List<Long> times = bucketTimes.get(bucket);
                            double avg = times.stream().mapToLong(Long::longValue).average().orElse(0.0) / 1e6;
                            System.out.printf("[%d%%] Inserted %,d patterns | Avg insert time: %.3f ms%n",
                                    (bucket + 1) * 10, index + 1, avg);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("Executor did not terminate in time.");
            }

            System.out.println("Insert complete.");
            System.out.printf("Insert avg %.3f ms | P50 %.3f | P95 %.3f | P99 %.3f%n",
                    allTimes.stream().mapToLong(Long::longValue).average().orElse(0) / 1e6,
                    percentile(allTimes, 50),
                    percentile(allTimes, 95),
                    percentile(allTimes, 99));
        }

        System.out.printf("Finished at: %s%n",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }

    @Test
    public void runTopKQueries() throws Exception {

        ResonanceEngine.setBackend(new JavaKernel());
        System.out.printf("=== TOP‑K Queries from %s ===%n", DB_ROOT);
        showSystemInfo();
        try (WavePatternStoreImpl store = new WavePatternStoreImpl(DB_ROOT)) {
            Path segmentsDir = DB_ROOT.resolve("segments");
            warmUpSSD(segmentsDir);

            long totalBytes = Files.walk(segmentsDir)
                    .filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0L;
                        }
                    }).sum();

            System.out.printf("Database size: %.2f MB%n", totalBytes / 1024.0 / 1024.0);
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int k : TOP_K_BUCKETS) {
                System.out.printf("Running %,d queries for top‑%d...\n", QUERY_REPS, k);
                List<Long> times = new ArrayList<>(QUERY_REPS);
                for (int i = 0; i < QUERY_REPS; i++) {
                    WavePattern q = randomPattern(PATTERN_LEN, rnd);
                    long t0 = System.nanoTime();
                    store.query(q, k);
                    times.add(System.nanoTime() - t0);
                }
                System.out.println();
                System.out.printf("TOP %3d | min %.3f ms | p50 %.3f | p95 %.3f | p99 %.3f | max %.3f | avg %.3f%n%n",
                        k,
                        Collections.min(times) / 1e6,
                        percentile(times, 50),
                        percentile(times, 95),
                        percentile(times, 99),
                        Collections.max(times) / 1e6,
                        times.stream().mapToLong(Long::longValue).average().orElse(0) / 1e6);
            }
            System.out.printf("Finished at: %s%n", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        }
    }
}