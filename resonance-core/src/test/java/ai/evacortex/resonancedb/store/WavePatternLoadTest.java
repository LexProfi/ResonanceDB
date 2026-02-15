/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import ai.evacortex.resonancedb.core.storage.responce.ResonanceMatch;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

//@Disabled("Smoke benchmark — run manually from IDE")
public class WavePatternLoadTest {

    private static final int PATTERN_COUNT = Integer.getInteger("patterns", 200_000);
    private static final int PATTERN_LEN = Integer.getInteger("length", 1536);
    private static final int QUERY_REPS = Integer.getInteger("queries", 100);
    private static final Path DB_ROOT = Paths.get(System.getProperty("resonance.test.dir", "D:\\ResonanceDB\\S64xL1536x200k"));

    private static final int[] TOP_K_BUCKETS = Arrays.stream(System.getProperty("resonance.test.topKs", "1,10,100").split(","))
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
        var rt = java.lang.management.ManagementFactory.getRuntimeMXBean();
        System.out.println("JVM args: " + rt.getInputArguments());
        System.out.println("Heap: max=" + (Runtime.getRuntime().maxMemory() / (1024*1024)) + " MB");
        System.out.println("Kernel: " + (Boolean.parseBoolean(System.getProperty("resonance.kernel.native")) ? "NativeKernel" : "JavaKernel"));
        long maxMemory = Runtime.getRuntime().maxMemory();
        long totalMemoryMB = maxMemory / (1024 * 1024);
        System.out.printf("RAM  : ~%d MB (max JVM heap)%n", totalMemoryMB);
    }

    @Test
    public void generateAndInsertPatterns() throws Exception {
        Files.createDirectories(DB_ROOT);
        System.out.printf("=== GENERATE %,d x L=%s patterns to %s ===%n", PATTERN_COUNT, PATTERN_LEN, DB_ROOT);
        showSystemInfo();

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

    @Test
    public void compareQueryLatencyWithAndWithoutPhaseRouting() throws Exception {
        System.out.printf("=== Phase-Routed vs Full-Scan Queries from %s ===%n", DB_ROOT);
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
                for (boolean fullScan : new boolean[]{true, false}) {
                    String label = fullScan ? "Full‑Scan" : "Phase‑Routed";
                    System.out.printf("Running %,d queries for top‑%d [%s]...\n", QUERY_REPS, k, label);
                    List<Long> times = new ArrayList<>(QUERY_REPS);

                    for (int i = 0; i < QUERY_REPS; i++) {
                        WavePattern q = randomPattern(PATTERN_LEN, rnd);
                        if (fullScan) {
                            double[] phase0 = new double[PATTERN_LEN];
                            Arrays.fill(phase0, 0.0);
                            q = new WavePattern(q.amplitude(), phase0);
                        }

                        long t0 = System.nanoTime();
                        store.query(q, k);
                        times.add(System.nanoTime() - t0);
                    }

                    System.out.printf("TOP %3d [%s] | p50 %.3f ms | p95 %.3f | avg %.3f%n%n",
                            k, label,
                            percentile(times, 50),
                            percentile(times, 95),
                            times.stream().mapToLong(Long::longValue).average().orElse(0) / 1e6);
                }
            }

            System.out.printf("Finished at: %s%n", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        }
    }

    @Test
    public void concurrentQueryLatencyStats() throws Exception {
        final int TOP_K = 10;
        final int THREADS = Runtime.getRuntime().availableProcessors() * 2;
        final int LOOPS = QUERY_REPS;
        final int WARMUP = Math.max(1, (int) (0.05 * LOOPS)); // 5% warmup

        System.out.printf("=== Concurrency TOP‑K Queries from %s ===%n", DB_ROOT);
        showSystemInfo();

        try (WavePatternStoreImpl store = new WavePatternStoreImpl(DB_ROOT)) {
            ExecutorService pool = Executors.newFixedThreadPool(THREADS);
            CountDownLatch latch = new CountDownLatch(THREADS);
            AtomicInteger failures = new AtomicInteger();
            List<List<Long>> allLatencies = new ArrayList<>(THREADS);

            for (int t = 0; t < THREADS; t++) {
                List<Long> local = new ArrayList<>(LOOPS - WARMUP);
                allLatencies.add(local);

                pool.submit(() -> {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    try {
                        for (int i = 0; i < LOOPS; i++) {
                            WavePattern q = randomPattern(PATTERN_LEN, rnd);
                            long start = System.nanoTime();
                            List<ResonanceMatch> hits = store.query(q, TOP_K);
                            long end = System.nanoTime();

                            if (i >= WARMUP) {
                                local.add(end - start);
                            }

                            if (hits.size() > TOP_K) {
                                failures.incrementAndGet();
                                continue;
                            }
                            float prev = Float.POSITIVE_INFINITY;
                            for (ResonanceMatch m : hits) {
                                float e = m.energy();
                                if (e < 0.0f || e > 1.0f || e > prev + 1e-5f) {
                                    failures.incrementAndGet();
                                    break;
                                }
                                prev = e;
                            }
                        }
                    } catch (Throwable ex) {
                        ex.printStackTrace(System.err);
                        failures.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            pool.shutdown();

            List<Long> samples = allLatencies.stream()
                    .flatMap(Collection::stream)
                    .sorted()
                    .toList();

            int count = samples.size();
            if (count == 0) throw new AssertionError("No latency samples collected");

            double avgMs = samples.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
            double p50 = samples.get(count / 2) / 1_000_000.0;
            double p95 = samples.get((int) (count * 0.95)) / 1_000_000.0;
            double p99 = samples.get((int) (count * 0.99)) / 1_000_000.0;
            double max = samples.get(count - 1) / 1_000_000.0;

            System.out.printf("""
                    Queries executed:  %,d
                    Avg latency   (ms): %.3f
                    P50 latency   (ms): %.3f
                    P95 latency   (ms): %.3f
                    P99 latency   (ms): %.3f
                    Max latency   (ms): %.3f
                    """, count, avgMs, p50, p95, p99, max);

            assertEquals(0, failures.get(), "Inaccuracies detected in concurrent results");
        }
    }

    @Test
    public void incrementalInsertQueryBenchmark() throws Exception {
        final int TOTAL = PATTERN_COUNT;
        final int CHUNK = Math.max(TOTAL / 10, 1);
        final int THREADS = Runtime.getRuntime().availableProcessors();

        System.out.printf("=== Incremental Insert + Query Benchmark ===%n");
        showSystemInfo();

        try (WavePatternStoreImpl store = new WavePatternStoreImpl(DB_ROOT)) {
            ExecutorService pool = Executors.newFixedThreadPool(THREADS);
            AtomicInteger inserted = new AtomicInteger();
            List<Long> insertTimes = Collections.synchronizedList(new ArrayList<>());
            List<Long> queryTimes = Collections.synchronizedList(new ArrayList<>());

            for (int phase = 1; phase <= 10; phase++) {
                int start = (phase - 1) * CHUNK;
                int end = Math.min(phase * CHUNK, TOTAL);
                CountDownLatch latch = new CountDownLatch(end - start);
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = start; i < end; i++) {
                    pool.submit(() -> {
                        try {
                            WavePattern wp = randomPattern(PATTERN_LEN, rnd);
                            long t0 = System.nanoTime();
                            store.insert(wp, Collections.emptyMap());
                            long t1 = System.nanoTime();
                            insertTimes.add(t1 - t0);
                            WavePattern q = randomPattern(PATTERN_LEN, rnd);
                            long t2 = System.nanoTime();
                            store.query(q, 10);
                            long t3 = System.nanoTime();
                            queryTimes.add(t3 - t2);
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                            inserted.incrementAndGet();
                        }
                    });
                }

                latch.await();

                System.out.printf("[%3d%%] DB size: %,d | Insert avg: %.3f ms | Query avg: %.3f ms | P99: %.3f ms%n",
                        phase * 10,
                        inserted.get(),
                        insertTimes.stream().mapToLong(Long::longValue).average().orElse(0) / 1e6,
                        queryTimes.stream().mapToLong(Long::longValue).average().orElse(0) / 1e6,
                        percentile(queryTimes, 99));

                insertTimes.clear();
                queryTimes.clear();
            }

            pool.shutdown();
            if (!pool.awaitTermination(3, TimeUnit.MINUTES)) {
                System.err.println("Executor did not terminate in time.");
            }
        }

        System.out.printf("Finished at: %s%n",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }
}