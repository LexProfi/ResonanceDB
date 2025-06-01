/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Alexander Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.nativeffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

public class NativeCompare {

    private static final Linker linker = Linker.nativeLinker();

    private static final FunctionDescriptor desc = FunctionDescriptor.of(
            ValueLayout.JAVA_FLOAT,    // return float
            ValueLayout.ADDRESS,       // amp1
            ValueLayout.ADDRESS,       // phase1
            ValueLayout.ADDRESS,       // amp2
            ValueLayout.ADDRESS,       // phase2
            ValueLayout.JAVA_INT       // len
    );

    private static final MethodHandle handle;

    static {
        SymbolLookup lookup = SymbolLookup.libraryLookup("resonance", Arena.global());
        handle = linker.downcallHandle(
                lookup.find("compare_wave_patterns").orElseThrow(),
                desc
        );
    }

    public static float compare(float[] amp1, float[] phase1,
                                float[] amp2, float[] phase2) throws Throwable {
        if (amp1.length != amp2.length || phase1.length != phase2.length || amp1.length != phase1.length) {
            throw new IllegalArgumentException("All arrays must be of equal length");
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a1 = toMemorySegment(amp1, arena);
            MemorySegment p1 = toMemorySegment(phase1, arena);
            MemorySegment a2 = toMemorySegment(amp2, arena);
            MemorySegment p2 = toMemorySegment(phase2, arena);

            return (float) handle.invoke(a1, p1, a2, p2, amp1.length);
        }
    }

    private static MemorySegment toMemorySegment(float[] array, Arena arena) {
        MemorySegment segment = arena.allocate(ValueLayout.JAVA_FLOAT, array.length);
        for (int i = 0; i < array.length; i++) {
            segment.setAtIndex(ValueLayout.JAVA_FLOAT, i, array[i]);
        }
        return segment;
    }
}