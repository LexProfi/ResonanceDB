/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.math.UnfoldedMath;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class PostingSidecar {

    private static final byte[] MAGIC = {'R', 'I', 'V', 'F'};
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 16;
    private static final int DIR_ENTRY_SIZE = 12;
    private static final int ID_BYTES = 32;

    public record PartitionEntry(long offset, int count) {}

    public static final class Loaded {
        private final MappedByteBuffer mmap;
        private final int partitionCount;
        private final int unfoldedDim;
        private final PartitionEntry[] directory;
        private final FileChannel channel;

        private Loaded(MappedByteBuffer mmap, FileChannel channel,
                       int partitionCount, int unfoldedDim, PartitionEntry[] directory) {
            this.mmap = mmap;
            this.channel = channel;
            this.partitionCount = partitionCount;
            this.unfoldedDim = unfoldedDim;
            this.directory = directory;
        }

        public void scanPartition(int partitionIdx, float[] queryU, float queryEnergy,
                                  List<ScoredCandidate> results) {
            if (partitionIdx < 0 || partitionIdx >= partitionCount) return;
            PartitionEntry entry = directory[partitionIdx];
            if (entry.count <= 0) return;

            int base = (int) entry.offset;
            int count = entry.count;
            int dim = unfoldedDim;

            int vectorsStart = base;
            int energiesStart = base + count * dim * 4;
            int idsStart = energiesStart + count * 4;

            float[] candVec = new float[dim];
            byte[] idBytes = new byte[ID_BYTES];

            for (int i = 0; i < count; i++) {
                int vecOffset = vectorsStart + i * dim * 4;
                for (int d = 0; d < dim; d++) {
                    candVec[d] = mmap.getFloat(vecOffset + d * 4);
                }

                float dot = UnfoldedMath.dotFloat32(queryU, candVec, 0, dim);
                float candEnergy = mmap.getFloat(energiesStart + i * 4);
                float approxScore = UnfoldedMath.scoreFromDotFloat32(dot, queryEnergy, candEnergy);

                int idOffset = idsStart + i * ID_BYTES;
                for (int b = 0; b < ID_BYTES; b++) {
                    idBytes[b] = mmap.get(idOffset + b);
                }

                results.add(new ScoredCandidate(
                        new String(idBytes, StandardCharsets.US_ASCII), approxScore));
            }
        }

        public int partitionCount() { return partitionCount; }
        public int unfoldedDim() { return unfoldedDim; }
        public int partitionSize(int idx) {
            return (idx >= 0 && idx < partitionCount) ? directory[idx].count : 0;
        }

        public void close() {
            try { channel.close(); } catch (IOException ignored) {}
        }
    }

    public record ScoredCandidate(String id, float approxScore) {}

    private PostingSidecar() {}

    public static void write(Path path, List<PartitionData> partitions, int unfoldedDim)
            throws IOException {
        int K = partitions.size();

        int dirSize = K * DIR_ENTRY_SIZE;
        int dataStart = HEADER_SIZE + dirSize;
        dataStart = ((dataStart + 63) / 64) * 64;

        PartitionEntry[] directory = new PartitionEntry[K];
        int currentOffset = dataStart;

        for (int p = 0; p < K; p++) {
            int count = partitions.get(p).count();
            directory[p] = new PartitionEntry(currentOffset, count);
            if (count > 0) {
                int blockSize = count * unfoldedDim * 4
                        + count * 4
                        + count * ID_BYTES;
                blockSize = ((blockSize + 63) / 64) * 64;
                currentOffset += blockSize;
            }
        }

        int totalSize = currentOffset;

        Path tmpFile = path.resolveSibling(path.getFileName() + ".tmp");
        Files.createDirectories(path.getParent());

        try (RandomAccessFile raf = new RandomAccessFile(tmpFile.toFile(), "rw");
             FileChannel ch = raf.getChannel()) {

            MappedByteBuffer buf = (MappedByteBuffer) ch
                    .map(FileChannel.MapMode.READ_WRITE, 0, totalSize)
                    .order(ByteOrder.LITTLE_ENDIAN);

            buf.put(MAGIC);
            buf.putInt(VERSION);
            buf.putInt(K);
            buf.putInt(unfoldedDim);

            for (PartitionEntry entry : directory) {
                buf.putLong(entry.offset);
                buf.putInt(entry.count);
            }

            for (int p = 0; p < K; p++) {
                PartitionData pd = partitions.get(p);
                if (pd.count() == 0) continue;

                buf.position((int) directory[p].offset);

                for (float[] vec : pd.vectors) {
                    for (float v : vec) {
                        buf.putFloat(v);
                    }
                }

                for (float e : pd.energies) {
                    buf.putFloat(e);
                }

                for (String id : pd.ids) {
                    byte[] idBytes = id.getBytes(StandardCharsets.US_ASCII);
                    buf.put(idBytes, 0, Math.min(idBytes.length, ID_BYTES));
                    for (int pad = idBytes.length; pad < ID_BYTES; pad++) {
                        buf.put((byte) 0);
                    }
                }
            }

            buf.force();
        }

        Files.move(tmpFile, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public static Loaded load(Path path) {
        if (!Files.exists(path)) return null;

        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            long fileSize = channel.size();
            if (fileSize < HEADER_SIZE) {
                channel.close();
                return null;
            }

            MappedByteBuffer mmap = (MappedByteBuffer) channel
                    .map(FileChannel.MapMode.READ_ONLY, 0, fileSize)
                    .order(ByteOrder.LITTLE_ENDIAN);

            byte[] magic = new byte[4];
            mmap.get(magic);
            if (magic[0] != MAGIC[0] || magic[1] != MAGIC[1] ||
                    magic[2] != MAGIC[2] || magic[3] != MAGIC[3]) {
                channel.close();
                return null;
            }

            int version = mmap.getInt();
            if (version != VERSION) { channel.close(); return null; }

            int partitionCount = mmap.getInt();
            int unfoldedDim = mmap.getInt();

            PartitionEntry[] directory = new PartitionEntry[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                long offset = mmap.getLong();
                int count = mmap.getInt();
                directory[i] = new PartitionEntry(offset, count);
            }

            return new Loaded(mmap, channel, partitionCount, unfoldedDim, directory);
        } catch (IOException e) {
            System.err.println("Failed to load posting sidecar: " + path + ": " + e.getMessage());
            return null;
        }
    }

    public static final class PartitionData {
        private final List<float[]> vectors;
        private final List<Float> energies;
        private final List<String> ids;

        public PartitionData() {
            this.vectors = new ArrayList<>();
            this.energies = new ArrayList<>();
            this.ids = new ArrayList<>();
        }

        public void add(String id, float[] unfoldedVector, float energy) {
            ids.add(id);
            vectors.add(unfoldedVector);
            energies.add(energy);
        }

        public int count() { return ids.size(); }
    }
}
