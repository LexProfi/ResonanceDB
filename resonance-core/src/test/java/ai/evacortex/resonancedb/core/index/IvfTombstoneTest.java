/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.exceptions.PatternNotFoundException;
import ai.evacortex.resonancedb.core.storage.ManifestIndex;
import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.core.storage.io.CachedReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentReader;
import ai.evacortex.resonancedb.core.storage.io.SegmentWriter;
import ai.evacortex.resonancedb.core.storage.util.HashingUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class IvfTombstoneTest {

    private static final int DIM = 32;
    private static final int CHECKSUM_LEN = 8;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("readWithIdFromBuffer throws PatternNotFoundException on tombstone")
    void tombstoneThrowsPatternNotFoundException() throws Exception {
        Path seg = tempDir.resolve("tombstone.seg");
        WavePattern pattern = randomPattern(DIM, 1L);
        String hexId = HashingUtil.md5Hex("tombstone-test");

        long offset;
        try (SegmentWriter writer = new SegmentWriter(seg, CHECKSUM_LEN)) {
            offset = writer.write(hexId, pattern);
            writer.markDeleted(offset);
            writer.flush();
        }

        try (CachedReader reader = CachedReader.open(seg)) {
            assertFalse(reader.allIds().contains(hexId),
                    "Tombstoned ID must not appear in allIds()");
        }

        try (SegmentReader segReader = new SegmentReader(seg)) {
            assertThrows(PatternNotFoundException.class,
                    () -> segReader.readWithId(offset),
                    "readWithId must throw PatternNotFoundException on tombstone");
        }
    }

    @Test
    @DisplayName("buildIndex skips tombstoned patterns without crashing")
    void buildIndexSurvivesTombstones() throws Exception {
        Path seg = tempDir.resolve("ivf-tombstone.seg");
        int patternCount = 30;
        List<String> hexIds = new ArrayList<>();
        Map<String, Long> offsets = new HashMap<>();

        try (SegmentWriter writer = new SegmentWriter(seg, CHECKSUM_LEN)) {
            for (int i = 0; i < patternCount; i++) {
                WavePattern p = randomPattern(DIM, i + 100L);
                String hexId = HashingUtil.md5Hex("pattern-" + i);
                long offset = writer.write(hexId, p);
                hexIds.add(hexId);
                offsets.put(hexId, offset);
            }
            writer.markDeleted(offsets.get(hexIds.get(0)));
            writer.markDeleted(offsets.get(hexIds.get(5)));
            writer.markDeleted(offsets.get(hexIds.get(10)));
            writer.flush();
        }

        CachedReader reader = CachedReader.open(seg);

        ManifestIndex manifest = ManifestIndex.loadOrCreate(
                tempDir.resolve("manifest.idx"));
        manifest.ensureFileExists();
        for (String hexId : hexIds) {
            manifest.add(hexId, "ivf-tombstone", 0L, 0.0);
        }

        assertEquals(patternCount - 3, reader.allIds().size(),
                "CachedReader should skip tombstoned entries");

        DeltaIndex delta = new DeltaIndex();
        CandidateSource fallback = new FullScanCandidateSource(
                List::of, segName -> null);
        CentroidIndex centroidIndex = null;
        IvfCandidateSource ivf = new IvfCandidateSource(
                centroidIndex, delta, manifest, 4, fallback);

        assertDoesNotThrow(() ->
                        ivf.buildIndex(
                                segName -> reader,
                                List.of("ivf-tombstone"),
                                DIM,
                                42L),
                "buildIndex must not crash on tombstoned patterns in manifest");

        reader.close();
    }

    @Test
    @DisplayName("buildIndex handles concurrent tombstone (race simulation)")
    void buildIndexHandlesConcurrentTombstone() throws Exception {
        Path seg = tempDir.resolve("race-tombstone.seg");
        int patternCount = 20;
        List<String> hexIds = new ArrayList<>();
        Map<String, Long> offsets = new HashMap<>();

        SegmentWriter writer = new SegmentWriter(seg, CHECKSUM_LEN);
        for (int i = 0; i < patternCount; i++) {
            WavePattern p = randomPattern(DIM, i + 200L);
            String hexId = HashingUtil.md5Hex("race-" + i);
            long offset = writer.write(hexId, p);
            hexIds.add(hexId);
            offsets.put(hexId, offset);
        }
        writer.flush();

        CachedReader reader = CachedReader.open(seg);
        assertEquals(patternCount, reader.allIds().size());

        writer.markDeleted(offsets.get(hexIds.get(3)));
        writer.markDeleted(offsets.get(hexIds.get(7)));
        writer.flush();

        assertTrue(reader.allIds().contains(hexIds.get(3)),
                "CachedReader offsetMap retains stale entry after concurrent tombstone");

        assertThrows(PatternNotFoundException.class,
                () -> reader.readById(hexIds.get(3)),
                "readById must throw for concurrently tombstoned pattern");

        ManifestIndex manifest = ManifestIndex.loadOrCreate(
                tempDir.resolve("manifest-race.idx"));
        manifest.ensureFileExists();
        for (String hexId : hexIds) {
            manifest.add(hexId, "race-tombstone", 0L, 0.0);
        }

        DeltaIndex delta = new DeltaIndex();
        CandidateSource fallback = new FullScanCandidateSource(
                List::of, segName -> null);
        IvfCandidateSource ivf = new IvfCandidateSource(
                null, delta, manifest, 4, fallback);

        assertDoesNotThrow(() ->
                        ivf.buildIndex(
                                segName -> reader,
                                List.of("race-tombstone"),
                                DIM,
                                42L),
                "buildIndex must handle concurrent tombstones gracefully");

        reader.close();
        writer.close();
    }

    private static WavePattern randomPattern(int dim, long seed) {
        Random rng = new Random(seed);
        double[] amp = new double[dim];
        double[] phase = new double[dim];
        for (int i = 0; i < dim; i++) {
            amp[i] = 0.1 + rng.nextDouble() * 0.9;
            phase[i] = rng.nextDouble() * 2 * Math.PI;
        }
        return new WavePattern(amp, phase);
    }
}
