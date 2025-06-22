package ai.evacortex.resonancedb.store;

import ai.evacortex.resonancedb.core.ResonanceStore;
import ai.evacortex.resonancedb.core.engine.JavaKernel;
import ai.evacortex.resonancedb.core.engine.ResonanceEngine;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.WavePatternStoreImpl;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class WavePatternLoadTest {

    // ---------- configurable -------------
    // ---------- configurable -------------
    private static final int PATTERN_COUNT = Integer.getInteger("patterns", 100_000);
    private static final int PATTERN_LEN = Integer.getInteger("length", 128);
    private static final int QUERY_REPS = Integer.getInteger("queries", 100);

    private static final int[] TOP_K_BUCKETS = Arrays.stream(System.getProperty("topKs", "1,10,100").split(","))
            .mapToInt(Integer::parseInt).toArray();

    // ---------- random helpers -----------
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
        List<Long> sorted = nanos.stream().sorted().toList();
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1))) / 1e6; // ms
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

    // -------------- benchmark ---------------
    //@Disabled("Smoke benchmark — run manually with -Dresdb.loadtest=true")
    @Test
    public void smokeLoadBenchmark() throws Exception {// отключен по умолчанию

        System.out.printf("=== ResonanceDB Smoke‑Load Benchmark ===%n");
        System.out.printf("OS   : %s %s%n", System.getProperty("os.name"), System.getProperty("os.version"));
        System.out.printf("CPU  : %d logical cores%n", Runtime.getRuntime().availableProcessors());
        System.out.printf("JVM  : %s %s%n", System.getProperty("java.vm.name"), System.getProperty("java.version"));
        ResonanceEngine.setBackend(new JavaKernel());
        //System.out.printf("Kernel: %s%n", ResonanceEngine.getBackend().getClass().getSimpleName());

        System.out.printf("Patterns : %,d (L = %d)%n", PATTERN_COUNT, PATTERN_LEN);
        System.out.printf("Queries  : %,d per bucket%n", QUERY_REPS);

        Path dbRoot = Files.createTempDirectory("resdb-load");
        dbRoot.toFile().deleteOnExit(); // гарантированная очистка
        try (WavePatternStoreImpl store = new WavePatternStoreImpl(dbRoot)) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            // ---------- insert phase ----------
            System.out.printf("Inserting %,d patterns...\n", PATTERN_COUNT);
            List<Long> insertTimes = new ArrayList<>(PATTERN_COUNT);
            for (int i = 0; i < PATTERN_COUNT; i++) {
                WavePattern wp = randomPattern(PATTERN_LEN, rnd);
                long t0 = System.nanoTime();
                store.insert(wp, Collections.emptyMap());
                insertTimes.add(System.nanoTime() - t0);

                if ((i + 1) % Math.max(1, PATTERN_COUNT / 100) == 0 || i == PATTERN_COUNT - 1) {
                    int percent = (i + 1) * 100 / PATTERN_COUNT;
                    System.out.printf("\rInsert progress: %3d%%", percent);
                }
            }
            System.out.println();
            System.out.printf("Insert P50 %.3f ms | P95 %.3f | P99 %.3f%n",
                    percentile(insertTimes, 50), percentile(insertTimes, 95), percentile(insertTimes, 99));

            warmUpSSD(dbRoot.resolve("segments"));

            // ---------- query phase ----------
            for (int k : TOP_K_BUCKETS) {
                System.out.printf("Running %,d queries for top‑%d...\n", QUERY_REPS, k);
                List<Long> times = new ArrayList<>(QUERY_REPS);
                for (int i = 0; i < QUERY_REPS; i++) {
                    WavePattern q = randomPattern(PATTERN_LEN, rnd);
                    long t0 = System.nanoTime();
                    store.query(q, k);
                    times.add(System.nanoTime() - t0);

                    if ((i + 1) % Math.max(1, QUERY_REPS / 100) == 0 || i == QUERY_REPS - 1) {
                        int percent = (i + 1) * 100 / QUERY_REPS;
                        System.out.printf("\rQuery progress (top‑%d): %3d%%", k, percent);
                    }
                }
                System.out.println();
                System.out.printf("TOP %3d | min %.3f ms | p50 %.3f | p95 %.3f | p99 %.3f | max %.3f | avg %.3f%n",
                        k,
                        Collections.min(times) / 1e6,
                        percentile(times, 50),
                        percentile(times, 95),
                        percentile(times, 99),
                        Collections.max(times) / 1e6,
                        times.stream().mapToLong(Long::longValue).average().orElse(0) / 1e6);
            }
        } finally {
            deleteRecursive(dbRoot);
        }
    }

    private static void deleteRecursive(Path root) throws IOException {
        if (!Files.exists(root)) return;
        Files.walk(root)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        System.err.printf("⚠ Failed to delete %s%n", p);
                    }
                });
    }
}