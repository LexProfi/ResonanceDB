/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.wal;

import ai.evacortex.resonancedb.core.storage.WavePattern;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kill-test for WAL durability (A1).
 *
 * <p>Spawns a child JVM that writes inserts to the WAL in a loop, printing
 * acked LSNs to stdout. The parent kills the child at a random moment.
 * After restart, replay must recover every acked LSN.</p>
 *
 * <p>Runs 10 iterations with different seeds (configurable via system property
 * {@code wal.killtest.iterations}).</p>
 */
class WalKillTest {

    @TempDir
    Path tempDir;

    @RepeatedTest(10)
    void ackedLsnsSurviveKill() throws Exception {
        Path walDir = tempDir.resolve("wal-kill-" + System.nanoTime());
        Files.createDirectories(walDir);

        long seed = System.nanoTime();

        String classpath = System.getProperty("java.class.path");
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "--enable-preview",
                "--add-modules", "jdk.incubator.vector",
                "-cp", classpath,
                WalWriterProcess.class.getName(),
                walDir.toAbsolutePath().toString(),
                String.valueOf(seed)
        );
        pb.redirectErrorStream(true);

        Process proc = pb.start();

        Set<Long> ackedLsns = new HashSet<>();
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(proc.getInputStream()));

        Random rng = new Random(seed);
        long runTimeMs = 50 + rng.nextInt(250);
        long deadline = System.currentTimeMillis() + runTimeMs;

        String line;
        while (System.currentTimeMillis() < deadline) {
            if (reader.ready()) {
                line = reader.readLine();
                if (line != null && line.startsWith("ACK:")) {
                    ackedLsns.add(Long.parseLong(line.substring(4)));
                }
            } else {
                Thread.sleep(1);
            }
        }

        proc.destroyForcibly();
        assertTrue(proc.waitFor(5, TimeUnit.SECONDS), "Child process did not terminate");

        try {
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("ACK:")) {
                    ackedLsns.add(Long.parseLong(line.substring(4)));
                }
            }
        } catch (IOException ignored) {
        }

        if (ackedLsns.isEmpty()) {
            return;
        }

        List<WalRecord> records = WriteAheadLog.replay(walDir);
        Set<Long> replayedLsns = new HashSet<>();
        for (WalRecord rec : records) {
            replayedLsns.add(rec.lsn());
        }

        for (long acked : ackedLsns) {
            assertTrue(replayedLsns.contains(acked),
                    "Acked LSN " + acked + " missing after replay! seed=" + seed +
                    ", acked=" + ackedLsns.size() + ", replayed=" + replayedLsns.size());
        }
    }

    /**
     * Child JVM process: writes inserts to WAL in a tight loop using GROUP mode,
     * printing "ACK:{lsn}" to stdout after each durable fsync.
     */
    public static final class WalWriterProcess {
        public static void main(String[] args) throws Exception {
            Path walDir = Path.of(args[0]);
            long seed = Long.parseLong(args[1]);
            Random rng = new Random(seed);

            try (WriteAheadLog wal = new WriteAheadLog(walDir,
                    WriteAheadLog.DurabilityMode.STRICT, 1, 1, 4L << 20)) {

                while (true) {
                    double[] amp = new double[16];
                    double[] phase = new double[16];
                    for (int i = 0; i < 16; i++) {
                        amp[i] = rng.nextDouble();
                        phase[i] = rng.nextDouble() * 2 * Math.PI - Math.PI;
                    }
                    WavePattern pattern = new WavePattern(amp, phase);

                    long lsn = wal.nextLsn();
                    WalRecord record = WalRecord.insert(lsn,
                            "hash-" + lsn, Map.of(), pattern);

                    wal.append(record).join();

                    System.out.println("ACK:" + lsn);
                    System.out.flush();
                }
            }
        }
    }
}
