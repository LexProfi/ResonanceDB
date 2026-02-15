/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.engine;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.foreign.SymbolLookup;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static java.lang.foreign.ValueLayout.*;

public final class NativeCompare {

    private static final Linker LINKER = Linker.nativeLinker();

    private static final FunctionDescriptor SCALAR_DESC = FunctionDescriptor.of(
            JAVA_FLOAT, ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT);

    private static final FunctionDescriptor BATCH_DESC = FunctionDescriptor.ofVoid(
            ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS);

    private static final FunctionDescriptor FLAT_DESC = FunctionDescriptor.ofVoid(
            ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS);

    private static final FunctionDescriptor DELTA_DESC = FunctionDescriptor.ofVoid(
            ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS);

    private static final MethodHandle SCALAR;
    private static final MethodHandle BATCH;
    private static final MethodHandle FLAT;
    private static final MethodHandle DELTA;

    static {
        loadNativeLibrary("resonance");
        try (Arena arena = Arena.ofConfined()) {
            SymbolLookup lookup = SymbolLookup.libraryLookup("resonance", arena);
            SCALAR = LINKER.downcallHandle(lookup.find("compare_wave_patterns").orElseThrow(), SCALAR_DESC);
            BATCH  = LINKER.downcallHandle(lookup.find("compare_many").orElseThrow(),           BATCH_DESC);
            FLAT   = LINKER.downcallHandle(lookup.find("compare_many_flat").orElseThrow(),      FLAT_DESC);
            DELTA  = LINKER.downcallHandle(lookup.find("compare_with_phase_delta").orElseThrow(), DELTA_DESC);
        }
    }

    private static void loadNativeLibrary(String base) {
        String os = System.getProperty("os.name").toLowerCase();
        String mapped = os.contains("win") ? base + ".dll"
                : os.contains("mac") ? "lib" + base + ".dylib"
                : "lib" + base + ".so";
        try {
            System.loadLibrary(base);
        } catch (UnsatisfiedLinkError e) {
            try (InputStream in = NativeCompare.class.getResourceAsStream("/native/" + mapped)) {
                if (in == null)
                    throw new IllegalStateException("Native library not found in resources: " + mapped, e);
                Path tmp = Files.createTempFile("resonance-", mapped.substring(mapped.lastIndexOf('.')));
                tmp.toFile().deleteOnExit();
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                System.load(tmp.toAbsolutePath().toString());
            } catch (IOException io) {
                throw new RuntimeException("Failed to load native lib from resources: " + mapped, io);
            }
        }
    }

    public static float compare(float[] amp1, float[] phase1, float[] amp2, float[] phase2) throws Throwable {
        validate(amp1, phase1, amp2, phase2);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a1 = arena.allocateFrom(JAVA_FLOAT, amp1);
            MemorySegment p1 = arena.allocateFrom(JAVA_FLOAT, phase1);
            MemorySegment a2 = arena.allocateFrom(JAVA_FLOAT, amp2);
            MemorySegment p2 = arena.allocateFrom(JAVA_FLOAT, phase2);
            return (float) SCALAR.invoke(a1, p1, a2, p2, amp1.length);
        }
    }

    public static float[] compareManyFlat(float[] ampQ, float[] phaseQ,
                                          float[] ampAll, float[] phaseAll,
                                          int len, int count) throws Throwable {
        validateFlat(ampQ, phaseQ, ampAll, phaseAll, len, count);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment qA   = arena.allocateFrom(JAVA_FLOAT, ampQ);
            MemorySegment qP   = arena.allocateFrom(JAVA_FLOAT, phaseQ);
            MemorySegment allA = arena.allocateFrom(JAVA_FLOAT, ampAll);
            MemorySegment allP = arena.allocateFrom(JAVA_FLOAT, phaseAll);

            MemorySegment out  = arena.allocate(JAVA_FLOAT, count);
            FLAT.invoke(qA, qP, allA, allP, len, count, out);
            return out.toArray(JAVA_FLOAT);
        }
    }

    public static float[] compareMany(float[] ampQ, float[] phaseQ,
                                      float[][] ampList, float[][] phaseList) throws Throwable {
        final int count = ampList.length;
        if (count == 0) return new float[0];

        if (phaseList.length != count) {
            throw new IllegalArgumentException("phaseList length mismatch");
        }
        final int len = ampQ.length;
        if (len == 0 || phaseQ.length != len) {
            throw new IllegalArgumentException("Query length mismatch");
        }
        for (int i = 0; i < count; i++) {
            if (ampList[i] == null || phaseList[i] == null)
                throw new IllegalArgumentException("Null at index " + i);
            if (ampList[i].length != len || phaseList[i].length != len)
                throw new IllegalArgumentException("Length mismatch at index " + i);
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment qA = arena.allocateFrom(JAVA_FLOAT, ampQ);
            MemorySegment qP = arena.allocateFrom(JAVA_FLOAT, phaseQ);

            final long elemSize = JAVA_FLOAT.byteSize();
            final long vecBytes = len * elemSize;
            final long allBytes = count * vecBytes;

            MemorySegment allA = arena.allocate(allBytes, Math.max(32, elemSize));
            MemorySegment allP = arena.allocate(allBytes, Math.max(32, elemSize));

            long dst = 0;
            for (int i = 0; i < count; i++, dst += vecBytes) {
                MemorySegment dstA = allA.asSlice(dst, vecBytes);
                MemorySegment dstP = allP.asSlice(dst, vecBytes);
                dstA.copyFrom(MemorySegment.ofArray(ampList[i]));
                dstP.copyFrom(MemorySegment.ofArray(phaseList[i]));
            }

            SequenceLayout ptrs = MemoryLayout.sequenceLayout(count, ADDRESS.withByteAlignment(8));
            MemorySegment ampPtrs   = arena.allocate(ptrs);
            MemorySegment phasePtrs = arena.allocate(ptrs);

            long off = 0;
            for (int i = 0; i < count; i++, off += vecBytes) {
                ampPtrs.setAtIndex(ADDRESS, i, allA.asSlice(off, vecBytes));
                phasePtrs.setAtIndex(ADDRESS, i, allP.asSlice(off, vecBytes));
            }

            MemorySegment out = arena.allocate(JAVA_FLOAT, count);
            BATCH.invoke(qA, qP, ampPtrs, phasePtrs, len, count, out);
            return out.toArray(JAVA_FLOAT);
        }
    }

    public static float[] compareWithPhaseDelta(float[] amp1, float[] phase1,
                                                float[] amp2, float[] phase2) throws Throwable {
        validate(amp1, phase1, amp2, phase2);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a1  = arena.allocateFrom(JAVA_FLOAT, amp1);
            MemorySegment p1  = arena.allocateFrom(JAVA_FLOAT, phase1);
            MemorySegment a2  = arena.allocateFrom(JAVA_FLOAT, amp2);
            MemorySegment p2  = arena.allocateFrom(JAVA_FLOAT, phase2);
            MemorySegment out = arena.allocate(JAVA_FLOAT, 2);
            DELTA.invoke(a1, p1, a2, p2, amp1.length, out);
            return out.toArray(JAVA_FLOAT);
        }
    }

    private static void validate(float[] a1, float[] p1, float[] a2, float[] p2) {
        if (a1 == null || p1 == null || a2 == null || p2 == null)
            throw new IllegalArgumentException("Null array");
        if (a1.length == 0 || p1.length == 0)
            throw new IllegalArgumentException("Empty array");
        if (a1.length != a2.length || p1.length != p2.length || a1.length != p1.length)
            throw new IllegalArgumentException("Length mismatch");
    }

    private static void validateFlat(float[] ampQ, float[] phaseQ,
                                     float[] ampAll, float[] phaseAll,
                                     int len, int count) {
        if (ampQ == null || phaseQ == null || ampAll == null || phaseAll == null)
            throw new IllegalArgumentException("Null input array");
        if (len <= 0)   throw new IllegalArgumentException("len must be > 0");
        if (count <= 0) throw new IllegalArgumentException("count must be > 0");
        if (ampQ.length != len || phaseQ.length != len)
            throw new IllegalArgumentException("Query vector length mismatch");
        long expected = (long) len * (long) count;
        if (ampAll.length != expected || phaseAll.length != expected)
            throw new IllegalArgumentException("Database matrix length mismatch");
    }

    private NativeCompare() {}
}