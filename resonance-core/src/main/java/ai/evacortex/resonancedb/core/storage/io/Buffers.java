/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.io;

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**  Utility — safe explicit unmap for a MappedByteBuffer.  */
@SuppressWarnings({"removal", "UnsafeUsage"})
final class Buffers {

    //TODO wait JEP 454
    private static final Unsafe UNSAFE;
    private static final java.lang.reflect.Method INVOKE_CLEANER;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);

            INVOKE_CLEANER = Unsafe.class.getMethod("invokeCleaner", ByteBuffer.class);
        } catch (Throwable t) {
            throw new RuntimeException("Cannot initialise explicit-unmap helper", t);
        }
    }

    private Buffers() {}

    static void unmap(ByteBuffer bb) {
        if (bb == null || !bb.isDirect()) return;
        try {
            INVOKE_CLEANER.invoke(UNSAFE, bb);
        } catch (Throwable t) {
            System.err.println("[WARN] explicit unmap failed: " + t);
        }
    }

    static MappedByteBuffer mmap(FileChannel channel, FileChannel.MapMode mode, long position, long size) {
        try {
            MappedByteBuffer buffer = channel.map(mode, position, size);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            return buffer;
        } catch (IOException e) {
            throw new RuntimeException("Failed to mmap segment", e);
        }
    }
}