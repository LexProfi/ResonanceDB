/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**  Utility — safe explicit unmap for a MappedByteBuffer.  */
final class Buffers {

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
}