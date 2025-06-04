/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core;

import ai.evacortex.resonancedb.core.io.codec.WavePatternCodec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class WavePatternCodecTest {

    @Test
    public void testSerialization_roundTrip() {
        WavePattern original = WavePatternTestUtils.fixedPattern();
        byte[] serialized = WavePatternCodec.serialize(original);
        WavePattern restored = WavePatternCodec.deserialize(serialized);
        assertArrayEquals(original.amplitude(), restored.amplitude(), 1e-9, "Amplitudes must match after roundtrip");
        assertArrayEquals(original.phase(), restored.phase(), 1e-9, "Phases must match after roundtrip");
    }
}