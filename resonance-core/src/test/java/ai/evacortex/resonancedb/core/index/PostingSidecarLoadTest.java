/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PostingSidecarLoadTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("load returns null for non-existent file without logging")
    void loadNonExistentFileReturnsNull() {
        Path nonExistent = tempDir.resolve("does-not-exist.ivf");

        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured));
        try {
            PostingSidecar.Loaded result = PostingSidecar.load(nonExistent);
            assertNull(result, "load() must return null for non-existent file");
            assertFalse(captured.toString().contains("Failed to load posting sidecar"),
                    "Non-existent file should not produce error log (TOCTOU fix)");
        } finally {
            System.setErr(originalErr);
        }
    }

    @Test
    @DisplayName("load returns null for file deleted between calls")
    void loadDeletedFileBetweenCallsReturnsNull() throws Exception {
        Path sidecar = tempDir.resolve("deleted.ivf");
        Files.write(sidecar, new byte[]{0x01, 0x02});
        assertTrue(Files.exists(sidecar));
        Files.delete(sidecar);

        PostingSidecar.Loaded result = PostingSidecar.load(sidecar);
        assertNull(result, "load() must return null after file deleted");
    }

    @Test
    @DisplayName("load returns null for file smaller than header")
    void loadTooSmallFileReturnsNull() throws Exception {
        Path tiny = tempDir.resolve("tiny.ivf");
        Files.write(tiny, new byte[]{0x01, 0x02, 0x03});

        PostingSidecar.Loaded result = PostingSidecar.load(tiny);
        assertNull(result, "load() must return null for file smaller than header");
    }

    @Test
    @DisplayName("load returns null for file with wrong magic bytes")
    void loadWrongMagicReturnsNull() throws Exception {
        Path bad = tempDir.resolve("bad-magic.ivf");
        byte[] data = new byte[64];
        data[0] = 'X'; data[1] = 'Y'; data[2] = 'Z'; data[3] = 'W';
        Files.write(bad, data);

        PostingSidecar.Loaded result = PostingSidecar.load(bad);
        assertNull(result, "load() must return null for wrong magic");
    }

    @Test
    @DisplayName("load-write-load roundtrip for valid sidecar")
    void loadWriteLoadRoundtrip() throws Exception {
        Path sidecar = tempDir.resolve("valid.ivf");
        int unfoldedDim = 64;

        PostingSidecar.PartitionData pd = new PostingSidecar.PartitionData();
        float[] vec = new float[unfoldedDim];
        for (int i = 0; i < unfoldedDim; i++) vec[i] = (float) i * 0.1f;
        pd.add("abc123", vec, 1.5f);

        PostingSidecar.write(sidecar, List.of(pd), unfoldedDim);
        assertTrue(Files.exists(sidecar));

        PostingSidecar.Loaded loaded = PostingSidecar.load(sidecar);
        assertNotNull(loaded, "Valid sidecar must load successfully");
        assertEquals(1, loaded.partitionCount());
        assertEquals(unfoldedDim, loaded.unfoldedDim());
        loaded.close();
    }
}
