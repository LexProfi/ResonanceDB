/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.store;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;


public class TestUtils {
    public static void deleteDirectoryRecursive(Path dir) throws IOException {
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}