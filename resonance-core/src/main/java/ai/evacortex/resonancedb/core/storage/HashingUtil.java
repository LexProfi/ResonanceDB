/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import net.jpountz.xxhash.XXHashFactory;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class HashingUtil {

    private static final XXHashFactory XX_HASH = XXHashFactory.fastestInstance();
    private static final int SEED = 0x9747b28c;

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

    public static long computeChecksum(ByteBuffer buf, int length) {
        if (length == 4) {
            return computeCRC32(buf);
        } else if (length == 8) {
            return computeXXHash64(buf);
        } else {
            throw new IllegalArgumentException("Unsupported checksum length: " + length);
        }
    }

    private static long computeCRC32(ByteBuffer buf) {
        Checksum crc = new CRC32();
        ByteBuffer copy = buf.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        crc.update(bytes, 0, bytes.length);
        return crc.getValue();
    }

    private static long computeXXHash64(ByteBuffer buf) {
        ByteBuffer copy = buf.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return XX_HASH.hash64().hash(bytes, 0, bytes.length, SEED);
    }
}

