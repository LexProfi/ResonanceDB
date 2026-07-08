/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.zip.CRC32C;

public final class CentroidPersistence {

    private static final byte[] MAGIC = {'R', 'C', 'I', 'X'};
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 4 + 4 + 4 + 4 + 8 + 4;

    private CentroidPersistence() {}

    public static void write(Path path, double[][] centroids, int dim, double lambda)
            throws IOException {
        int k = centroids.length;
        long dataBytesLong = (long) k * dim * 8;
        if (dataBytesLong > Integer.MAX_VALUE) {
            throw new IOException("Centroid data exceeds 2GB: " + dataBytesLong + " bytes");
        }
        int dataBytes = (int) dataBytesLong;

        CRC32C crc = new CRC32C();
        ByteBuffer dataBuf = ByteBuffer.allocate(dataBytes).order(ByteOrder.LITTLE_ENDIAN);
        for (double[] centroid : centroids) {
            for (double v : centroid) {
                dataBuf.putDouble(v);
            }
        }
        dataBuf.flip();
        crc.update(dataBuf);
        int crcValue = (int) crc.getValue();

        Path tmpFile = path.resolveSibling(path.getFileName() + ".tmp");
        Files.createDirectories(path.getParent());

        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(tmpFile))) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            header.put(MAGIC);
            header.putInt(VERSION);
            header.putInt(dim);
            header.putInt(k);
            header.putDouble(lambda);
            header.putInt(crcValue);
            header.flip();

            os.write(header.array());

            dataBuf.rewind();
            byte[] dataArray = new byte[dataBuf.remaining()];
            dataBuf.get(dataArray);
            os.write(dataArray);
        }

        Files.move(tmpFile, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public static CentroidIndex read(Path path) {
        if (!Files.exists(path)) {
            return null;
        }

        try {
            byte[] bytes = Files.readAllBytes(path);
            if (bytes.length < HEADER_SIZE) {
                return null;
            }

            ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

            byte[] magic = new byte[4];
            buf.get(magic);
            if (magic[0] != MAGIC[0] || magic[1] != MAGIC[1] ||
                    magic[2] != MAGIC[2] || magic[3] != MAGIC[3]) {
                return null;
            }

            int version = buf.getInt();
            if (version != VERSION) {
                return null;
            }

            int dim = buf.getInt();
            int k = buf.getInt();
            double lambda = buf.getDouble();
            int expectedCrc = buf.getInt();

            long dataBytesLong = (long) k * dim * 8;
            if (dataBytesLong > Integer.MAX_VALUE || bytes.length < HEADER_SIZE + dataBytesLong) {
                return null;
            }
            int dataBytes = (int) dataBytesLong;

            CRC32C crc = new CRC32C();
            crc.update(bytes, HEADER_SIZE, dataBytes);
            if ((int) crc.getValue() != expectedCrc) {
                return null;
            }

            double[][] centroids = new double[k][dim];
            for (int c = 0; c < k; c++) {
                for (int d = 0; d < dim; d++) {
                    centroids[c][d] = buf.getDouble();
                }
            }

            return new CentroidIndex(centroids, dim, lambda);

        } catch (IOException e) {
            System.err.println("Failed to read centroids from " + path + ": " + e.getMessage());
            return null;
        }
    }
}
