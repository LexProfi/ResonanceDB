/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.nativeffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.ValueLayout.*;

public final class NativeCompare {

    private static final Linker linker = Linker.nativeLinker();

    private static final FunctionDescriptor scalarDesc = FunctionDescriptor.of(
            JAVA_FLOAT,
            ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT
    );

    private static final FunctionDescriptor batchDesc = FunctionDescriptor.ofVoid(
            ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS
    );

    private static final FunctionDescriptor deltaDesc = FunctionDescriptor.ofVoid(
            ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS
    );

    private static final MethodHandle deltaHandle;

    private static final MethodHandle scalarHandle;
    private static final MethodHandle batchHandle;

    static {
        SymbolLookup lookup = SymbolLookup.libraryLookup("resonance", Arena.global());

        scalarHandle = linker.downcallHandle(
                lookup.find("compare_wave_patterns").orElseThrow(() ->
                        new UnsatisfiedLinkError("Native symbol 'compare_wave_patterns' not found")),
                scalarDesc
        );

        batchHandle = linker.downcallHandle(
                lookup.find("compare_many").orElseThrow(() ->
                        new UnsatisfiedLinkError("Native symbol 'compare_many' not found")),
                batchDesc
        );

        deltaHandle = linker.downcallHandle(
                lookup.find("compare_with_phase_delta").orElseThrow(() ->
                        new UnsatisfiedLinkError("Native symbol 'compare_with_phase_delta' not found")),
                deltaDesc
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

            return (float) scalarHandle.invoke(a1, p1, a2, p2, amp1.length);
        }
    }

    public static float[] compareMany(float[] ampQ, float[] phaseQ,
                                      float[][] ampList, float[][] phaseList) throws Throwable {

        int count = ampList.length;
        if (count != phaseList.length) {
            throw new IllegalArgumentException("Mismatched candidate array count");
        }

        int len = ampQ.length;
        for (int i = 0; i < count; i++) {
            if (ampList[i].length != len || phaseList[i].length != len) {
                throw new IllegalArgumentException("All candidate arrays must have length = " + len);
            }
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment ampQSeg = toMemorySegment(ampQ, arena);
            MemorySegment phaseQSeg = toMemorySegment(phaseQ, arena);

            VarHandle addressHandle = ADDRESS.varHandle();

            MemorySegment ampPtrs = arena.allocate(ADDRESS, count);
            MemorySegment phasePtrs = arena.allocate(ADDRESS, count);
            MemorySegment output = arena.allocate(JAVA_FLOAT, count);

            for (int i = 0; i < count; i++) {
                MemorySegment ampSeg = toMemorySegment(ampList[i], arena);
                MemorySegment phaseSeg = toMemorySegment(phaseList[i], arena);

                addressHandle.set(ampPtrs.asSlice(i * ADDRESS.byteSize()), ampSeg);
                addressHandle.set(phasePtrs.asSlice(i * ADDRESS.byteSize()), phaseSeg);
            }

            batchHandle.invoke(ampQSeg, phaseQSeg, ampPtrs, phasePtrs, len, count, output);

            float[] result = new float[count];
            for (int i = 0; i < count; i++) {
                result[i] = output.getAtIndex(JAVA_FLOAT, i);
            }

            return result;
        }
    }

    public static float[] compareWithPhaseDelta(float[] amp1, float[] phase1,
                                                float[] amp2, float[] phase2) throws Throwable {
        if (amp1.length != amp2.length || phase1.length != phase2.length || amp1.length != phase1.length) {
            throw new IllegalArgumentException("All arrays must be of equal length");
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a1 = toMemorySegment(amp1, arena);
            MemorySegment p1 = toMemorySegment(phase1, arena);
            MemorySegment a2 = toMemorySegment(amp2, arena);
            MemorySegment p2 = toMemorySegment(phase2, arena);
            MemorySegment out = arena.allocate(JAVA_FLOAT, 2);

            deltaHandle.invoke(a1, p1, a2, p2, amp1.length, out);

            return new float[]{
                    out.getAtIndex(JAVA_FLOAT, 0),
                    out.getAtIndex(JAVA_FLOAT, 1)
            };
        }
    }

    private static MemorySegment toMemorySegment(float[] array, Arena arena) {
        MemorySegment segment = arena.allocate(JAVA_FLOAT, array.length);
        for (int i = 0; i < array.length; i++) {
            segment.setAtIndex(JAVA_FLOAT, i, array[i]);
        }
        return segment;
    }
}