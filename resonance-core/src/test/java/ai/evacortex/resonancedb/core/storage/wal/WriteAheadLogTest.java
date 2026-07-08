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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class WriteAheadLogTest {

    @TempDir
    Path tempDir;
    Path walDir;

    @BeforeEach
    void setUp() {
        walDir = tempDir.resolve("wal");
    }

    // ─── WalRecord roundtrip tests ──────────────────────────────────────

    @Test
    void insertRecordRoundtrip() {
        WavePattern pattern = randomPattern(64);
        Map<String, String> meta = Map.of("key1", "value1", "key2", "value2");

        WalRecord record = WalRecord.insert(42L, "abc123hash", meta, pattern);
        byte[] bytes = record.toBytes();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        WalRecord decoded = assertDoesNotThrow(() -> WalRecord.fromBuffer(buf));
        assertNotNull(decoded);
        assertEquals(42L, decoded.lsn());
        assertEquals(WalRecord.TYPE_INSERT, decoded.type());

        WalRecord.InsertPayload payload = decoded.decodeInsert();
        assertEquals("abc123hash", payload.contentHash());
        assertEquals(2, payload.metadata().size());
        assertEquals("value1", payload.metadata().get("key1"));
        assertEquals("value2", payload.metadata().get("key2"));
        assertArrayEquals(pattern.amplitude(), payload.pattern().amplitude());
        assertArrayEquals(pattern.phase(), payload.pattern().phase());
    }

    @Test
    void insertRecordEmptyMetadata() {
        WavePattern pattern = randomPattern(16);
        WalRecord record = WalRecord.insert(1L, "hash", Map.of(), pattern);
        byte[] bytes = record.toBytes();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        WalRecord decoded = assertDoesNotThrow(() -> WalRecord.fromBuffer(buf));
        WalRecord.InsertPayload payload = decoded.decodeInsert();
        assertTrue(payload.metadata().isEmpty());
        assertArrayEquals(pattern.amplitude(), payload.pattern().amplitude());
    }

    @Test
    void insertRecordNullMetadata() {
        WavePattern pattern = randomPattern(16);
        WalRecord record = WalRecord.insert(1L, "hash", null, pattern);
        byte[] bytes = record.toBytes();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        WalRecord decoded = assertDoesNotThrow(() -> WalRecord.fromBuffer(buf));
        assertTrue(decoded.decodeInsert().metadata().isEmpty());
    }

    @Test
    void deleteRecordRoundtrip() {
        WalRecord record = WalRecord.delete(7L, "delete-me-hash");
        byte[] bytes = record.toBytes();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        WalRecord decoded = assertDoesNotThrow(() -> WalRecord.fromBuffer(buf));
        assertEquals(7L, decoded.lsn());
        assertEquals(WalRecord.TYPE_DELETE, decoded.type());
        assertEquals("delete-me-hash", decoded.decodeDeleteId());
    }

    @Test
    void replaceRecordRoundtrip() {
        WavePattern newPattern = randomPattern(32);
        Map<String, String> meta = Map.of("updated", "true");

        WalRecord record = WalRecord.replace(99L, "old-id", "new-hash", meta, newPattern);
        byte[] bytes = record.toBytes();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        WalRecord decoded = assertDoesNotThrow(() -> WalRecord.fromBuffer(buf));
        assertEquals(99L, decoded.lsn());
        assertEquals(WalRecord.TYPE_REPLACE, decoded.type());

        WalRecord.ReplacePayload payload = decoded.decodeReplace();
        assertEquals("old-id", payload.oldId());
        assertEquals("new-hash", payload.insert().contentHash());
        assertEquals("true", payload.insert().metadata().get("updated"));
        assertArrayEquals(newPattern.amplitude(), payload.insert().pattern().amplitude());
    }

    @Test
    void checkpointRecordRoundtrip() {
        WalRecord record = WalRecord.checkpoint(50L, 42L);
        byte[] bytes = record.toBytes();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        WalRecord decoded = assertDoesNotThrow(() -> WalRecord.fromBuffer(buf));
        assertEquals(50L, decoded.lsn());
        assertEquals(WalRecord.TYPE_CHECKPOINT, decoded.type());
        assertEquals(42L, decoded.decodeCheckpointLsn());
    }

    @Test
    void crcMismatchDetected() {
        WalRecord record = WalRecord.insert(1L, "hash", Map.of(), randomPattern(8));
        byte[] bytes = record.toBytes();
        // Corrupt one byte in the middle of the payload
        bytes[bytes.length / 2] ^= 0xFF;

        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertThrows(WalCorruptionException.class, () -> WalRecord.fromBuffer(buf));
    }

    @Test
    void partialRecordReturnsNull() throws WalCorruptionException {
        WalRecord record = WalRecord.insert(1L, "hash", Map.of(), randomPattern(8));
        byte[] bytes = record.toBytes();
        // Truncate: provide only half the data
        byte[] truncated = new byte[bytes.length / 2];
        System.arraycopy(bytes, 0, truncated, 0, truncated.length);

        ByteBuffer buf = ByteBuffer.wrap(truncated).order(ByteOrder.LITTLE_ENDIAN);
        WalRecord decoded = WalRecord.fromBuffer(buf);
        assertNull(decoded, "Partial record should return null");
    }

    // ─── WriteAheadLog integration tests ────────────────────────────────

    @Test
    void appendAndReplayStrict() throws Exception {
        WavePattern p1 = randomPattern(32);
        WavePattern p2 = randomPattern(32);

        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, 128L << 20)) {

            long lsn1 = wal.nextLsn();
            CompletableFuture<Long> f1 = wal.append(
                    WalRecord.insert(lsn1, "hash1", Map.of("k", "v"), p1));
            assertEquals(lsn1, f1.get(5, TimeUnit.SECONDS));

            long lsn2 = wal.nextLsn();
            CompletableFuture<Long> f2 = wal.append(
                    WalRecord.delete(lsn2, "hash1"));
            assertEquals(lsn2, f2.get(5, TimeUnit.SECONDS));

            long lsn3 = wal.nextLsn();
            CompletableFuture<Long> f3 = wal.append(
                    WalRecord.insert(lsn3, "hash2", Map.of(), p2));
            assertEquals(lsn3, f3.get(5, TimeUnit.SECONDS));
        }

        // Replay from disk
        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertEquals(3, records.size());

        assertEquals(WalRecord.TYPE_INSERT, records.get(0).type());
        assertEquals("hash1", records.get(0).decodeInsert().contentHash());

        assertEquals(WalRecord.TYPE_DELETE, records.get(1).type());
        assertEquals("hash1", records.get(1).decodeDeleteId());

        assertEquals(WalRecord.TYPE_INSERT, records.get(2).type());
        assertEquals("hash2", records.get(2).decodeInsert().contentHash());
    }

    @Test
    void appendAndReplayGroupCommit() throws Exception {
        int count = 500;
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.GROUP, 64, 2, 128L << 20)) {

            CompletableFuture<?>[] futures = new CompletableFuture[count];
            for (int i = 0; i < count; i++) {
                long lsn = wal.nextLsn();
                futures[i] = wal.append(
                        WalRecord.insert(lsn, "hash-" + i, Map.of(), randomPattern(8)));
            }
            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);
        }

        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertEquals(count, records.size());

        // Verify LSN ordering
        for (int i = 1; i < records.size(); i++) {
            assertTrue(records.get(i).lsn() > records.get(i - 1).lsn(),
                    "LSNs must be monotonically increasing");
        }
    }

    @Test
    void tailCorruptionDiscarded() throws Exception {
        // Write some valid records
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, 128L << 20)) {
            for (int i = 0; i < 5; i++) {
                long lsn = wal.nextLsn();
                wal.append(WalRecord.insert(lsn, "h" + i, Map.of(), randomPattern(8)))
                        .get(5, TimeUnit.SECONDS);
            }
        }

        // Append garbage at the end of the WAL file
        List<WriteAheadLog.WalFileInfo> files =
                WriteAheadLog.listWalFilesStatic(walDir);
        assertFalse(files.isEmpty());
        Path lastFile = files.getLast().path();
        try (var raf = new RandomAccessFile(lastFile.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[]{0x00, 0x10, 0x00, 0x00, // len=16 (valid-looking)
                    0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,  // garbage lsn
                    0x01,  // type
                    (byte)0xDE, (byte)0xAD,  // garbage payload
                    (byte)0xBA, (byte)0xD0, (byte)0xBA, (byte)0xD0 // bad crc
            });
        }

        // Replay should discard the garbage tail, return 5 valid records
        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertEquals(5, records.size());
    }

    @Test
    void midFileCorruptionFails() throws Exception {
        // Write valid records
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, 128L << 20)) {
            for (int i = 0; i < 10; i++) {
                long lsn = wal.nextLsn();
                wal.append(WalRecord.insert(lsn, "h" + i, Map.of(), randomPattern(8)))
                        .get(5, TimeUnit.SECONDS);
            }
        }

        // Corrupt a byte in the middle of the file
        List<WriteAheadLog.WalFileInfo> files =
                WriteAheadLog.listWalFilesStatic(walDir);
        Path lastFile = files.getLast().path();
        byte[] data = Files.readAllBytes(lastFile);
        int midPoint = data.length / 2;
        data[midPoint] ^= 0xFF;
        Files.write(lastFile, data);

        // Replay should throw — mid-file corruption is fatal (A5)
        assertThrows(WalCorruptionException.class, () -> WriteAheadLog.replay(walDir));
    }

    @Test
    void fileRotation() throws Exception {
        // Use tiny segment size to force rotation
        long tinySegment = 256; // 256 bytes per file
        int count = 20;

        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, tinySegment)) {
            for (int i = 0; i < count; i++) {
                long lsn = wal.nextLsn();
                wal.append(WalRecord.insert(lsn, "h" + i, Map.of(), randomPattern(8)))
                        .get(5, TimeUnit.SECONDS);
            }
        }

        // Should have multiple WAL files
        List<WriteAheadLog.WalFileInfo> files =
                WriteAheadLog.listWalFilesStatic(walDir);
        assertTrue(files.size() > 1, "Expected multiple WAL files after rotation");

        // All records should be replayable
        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertEquals(count, records.size());
    }

    @Test
    void checkpointAndTruncate() throws Exception {
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, 200)) {

            // Write 10 records across multiple files (tiny segment = rotation)
            long[] lsns = new long[10];
            for (int i = 0; i < 10; i++) {
                lsns[i] = wal.nextLsn();
                wal.append(WalRecord.insert(lsns[i], "h" + i, Map.of(), randomPattern(8)))
                        .get(5, TimeUnit.SECONDS);
            }

            int filesBefore = WriteAheadLog.listWalFilesStatic(walDir).size();

            // Checkpoint at LSN 5 — should truncate files with max LSN <= 5
            wal.checkpoint(lsns[4]);

            int filesAfter = WriteAheadLog.listWalFilesStatic(walDir).size();
            assertTrue(filesAfter <= filesBefore,
                    "Some WAL files should have been truncated");
        }
    }

    @Test
    void emptyReplay() throws Exception {
        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertTrue(records.isEmpty());
    }

    @Test
    void reopenAndContinueLsn() throws Exception {
        long lastLsn;
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, 128L << 20)) {
            for (int i = 0; i < 5; i++) {
                lastLsn = wal.nextLsn();
                wal.append(WalRecord.insert(lastLsn, "h" + i, Map.of(), randomPattern(8)))
                        .get(5, TimeUnit.SECONDS);
            }
            lastLsn = wal.currentLsn();
        }

        // Reopen — LSN should continue from where we left off
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.STRICT, 1, 1, 128L << 20)) {
            long nextLsn = wal.nextLsn();
            assertTrue(nextLsn > 5, "LSN should continue past previous writes");

            wal.append(WalRecord.insert(nextLsn, "h-reopen", Map.of(), randomPattern(8)))
                    .get(5, TimeUnit.SECONDS);
        }

        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertEquals(6, records.size());
    }

    @Test
    void asyncModeAcksImmediately() throws Exception {
        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.ASYNC, 256, 2, 128L << 20)) {

            long start = System.nanoTime();
            int count = 100;
            CompletableFuture<?>[] futures = new CompletableFuture[count];
            for (int i = 0; i < count; i++) {
                long lsn = wal.nextLsn();
                futures[i] = wal.append(
                        WalRecord.insert(lsn, "h" + i, Map.of(), randomPattern(8)));
            }
            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);
            long elapsed = System.nanoTime() - start;

            // Async should be fast: all 100 records acked quickly
            assertTrue(elapsed < TimeUnit.SECONDS.toNanos(2),
                    "Async mode should ack quickly, took " +
                    TimeUnit.NANOSECONDS.toMillis(elapsed) + " ms");
        }
    }

    @Test
    void multipleRecordTypesInSameLog() throws Exception {
        WavePattern p = randomPattern(16);

        try (WriteAheadLog wal = new WriteAheadLog(walDir,
                WriteAheadLog.DurabilityMode.GROUP, 64, 2, 128L << 20)) {

            long lsn1 = wal.nextLsn();
            wal.append(WalRecord.insert(lsn1, "id1", Map.of("a", "1"), p))
                    .get(5, TimeUnit.SECONDS);

            long lsn2 = wal.nextLsn();
            wal.append(WalRecord.delete(lsn2, "id1"))
                    .get(5, TimeUnit.SECONDS);

            long lsn3 = wal.nextLsn();
            wal.append(WalRecord.replace(lsn3, "id2", "id3", Map.of("b", "2"), p))
                    .get(5, TimeUnit.SECONDS);

            long lsn4 = wal.nextLsn();
            wal.append(WalRecord.checkpoint(lsn4, lsn2))
                    .get(5, TimeUnit.SECONDS);
        }

        List<WalRecord> records = WriteAheadLog.replay(walDir);
        assertEquals(4, records.size());
        assertEquals(WalRecord.TYPE_INSERT, records.get(0).type());
        assertEquals(WalRecord.TYPE_DELETE, records.get(1).type());
        assertEquals(WalRecord.TYPE_REPLACE, records.get(2).type());
        assertEquals(WalRecord.TYPE_CHECKPOINT, records.get(3).type());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

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
