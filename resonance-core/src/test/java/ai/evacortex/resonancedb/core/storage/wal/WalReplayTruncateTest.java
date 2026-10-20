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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class WalReplayTruncateTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("checkpoint after replay truncates old WAL segment files")
    void checkpointAfterReplayTruncatesOldFiles() throws Exception {
        Path walDir = tempDir.resolve("wal");

        long tinySegment = 200;
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, tinySegment)) {
            for (int i = 0; i < 20; i++) {
                long lsn = wal.nextLsn();
                wal.append(WalRecord.insert(lsn, "h" + i, Map.of(), randomPattern(8)))
                        .get(5, TimeUnit.SECONDS);
            }
        }

        List<WriteAheadLog.WalFileInfo> filesBeforeReplay =
                WriteAheadLog.listWalFilesStatic(walDir);
        assertTrue(filesBeforeReplay.size() > 1,
                "Tiny segment size should produce multiple WAL files");

        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertEquals(20, records.size());

        long maxLsn = records.stream().mapToLong(WalRecord::lsn).max().orElse(0);
        assertTrue(maxLsn > 0);

        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, tinySegment)) {
            wal.checkpoint(maxLsn);
        }

        List<WriteAheadLog.WalFileInfo> filesAfterCheckpoint =
                WriteAheadLog.listWalFilesStatic(walDir);
        assertTrue(filesAfterCheckpoint.size() < filesBeforeReplay.size(),
                "Checkpoint after replay must truncate old WAL files: before=" +
                        filesBeforeReplay.size() + " after=" + filesAfterCheckpoint.size());
    }

    @Test
    @DisplayName("replay + checkpoint + replay returns only checkpoint record")
    void replayAfterCheckpointReturnsFewer() throws Exception {
        Path walDir = tempDir.resolve("wal2");

        long tinySegment = 200;
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, tinySegment)) {
            for (int i = 0; i < 15; i++) {
                long lsn = wal.nextLsn();
                wal.append(WalRecord.insert(lsn, "h" + i, Map.of(), randomPattern(8)))
                        .get(5, TimeUnit.SECONDS);
            }
        }

        List<WalRecord> firstReplay = WriteAheadLog.replay(walDir);
        assertEquals(15, firstReplay.size(), "First replay must return all 15 records");

        long maxLsn = firstReplay.stream().mapToLong(WalRecord::lsn).max().orElse(0);

        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, tinySegment)) {
            wal.checkpoint(maxLsn);
        }

        List<WalRecord> secondReplay = WriteAheadLog.replay(walDir);
        assertTrue(secondReplay.size() < firstReplay.size(),
                "Second replay after checkpoint must return fewer records: first=" +
                        firstReplay.size() + " second=" + secondReplay.size());
    }

    @Test
    @DisplayName("replay returns empty for non-existent WAL directory")
    void replayNonExistentDirReturnsEmpty() throws Exception {
        Path walDir = tempDir.resolve("no-such-wal");
        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertTrue(records.isEmpty());
    }

    private static final Random RNG = new Random(42);

    private static WavePattern randomPattern(int len) {
        double[] amp = new double[len];
        double[] phase = new double[len];
        for (int i = 0; i < len; i++) {
            amp[i] = RNG.nextDouble();
            phase[i] = RNG.nextDouble() * 2 * Math.PI - Math.PI;
        }
        return new WavePattern(amp, phase);
    }
}
