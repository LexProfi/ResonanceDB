/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.storage;

import ai.evacortex.resonancedb.core.WavePattern;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public class HashingUtil {

    private static final ThreadLocal<MessageDigest> MD5_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    });

    public static byte[] md5(String input) {
        return MD5_DIGEST.get().digest(input.getBytes());
    }

    public static String md5Hex(String input) {
        return HexFormat.of().formatHex(md5(input));
    }

    public static String computeContentHash(WavePattern pattern) {
        if (pattern.amplitude().length != pattern.phase().length) {
            throw new IllegalArgumentException("Amplitude and phase arrays must be of equal length.");
        }
        MessageDigest digest = MD5_DIGEST.get();
        digest.reset();

        double[] amp = pattern.amplitude();
        double[] phase = pattern.phase();

        ByteBuffer buffer = ByteBuffer.allocate(amp.length * 16);
        for (int i = 0; i < amp.length; i++) {
            buffer.putDouble(amp[i]);
            buffer.putDouble(phase[i]);
        }

        byte[] hash = digest.digest(buffer.array());
        return HexFormat.of().formatHex(hash);
    }

    public static byte[] parseAndValidateMd5(String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex);
        if (bytes.length != 16) throw new IllegalArgumentException("Invalid MD5 hex length");
        return bytes;
    }
}

