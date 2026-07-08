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

public final class VamanaPersistence {

    private static final byte[] MAGIC = {'R', 'V', 'G', 'X'};
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 24;

    private VamanaPersistence() {}

    public static void write(Path path, VamanaGraph graph) throws IOException {
        Path tmpFile = path.resolveSibling(path.getFileName() + ".tmp");
        Files.createDirectories(path.getParent());

        byte[] nodeData;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream data = new DataOutputStream(baos)) {
            for (int i = 0; i < graph.nodeCount(); i++) {
                byte[] idBytes = graph.patternIds()[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
                data.writeShort(idBytes.length);
                data.write(idBytes);

                int[] nbrs = graph.neighbors()[i];
                data.writeShort(nbrs != null ? nbrs.length : 0);
                if (nbrs != null) {
                    for (int nbr : nbrs) {
                        data.writeInt(nbr);
                    }
                }
            }
            data.flush();
            nodeData = baos.toByteArray();
        }

        CRC32C crc = new CRC32C();
        crc.update(nodeData);
        int crcValue = (int) crc.getValue();

        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(tmpFile))) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            header.put(MAGIC);
            header.putInt(VERSION);
            header.putInt(graph.nodeCount());
            header.putInt(graph.maxDegree());
            header.putInt(graph.medoid());
            header.putInt(crcValue);
            header.flip();
            os.write(header.array());
            os.write(nodeData);
        }

        Files.move(tmpFile, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public static VamanaGraph read(Path path) {
        if (!Files.exists(path)) return null;

        try {
            byte[] bytes = Files.readAllBytes(path);
            if (bytes.length < HEADER_SIZE) return null;

            ByteBuffer header = ByteBuffer.wrap(bytes, 0, HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            byte[] magic = new byte[4];
            header.get(magic);
            if (magic[0] != MAGIC[0] || magic[1] != MAGIC[1] ||
                    magic[2] != MAGIC[2] || magic[3] != MAGIC[3]) return null;

            int version = header.getInt();
            if (version != VERSION) return null;

            int nodeCount = header.getInt();
            int maxDegree = header.getInt();
            int medoid = header.getInt();
            int expectedCrc = header.getInt();

            byte[] nodeData = new byte[bytes.length - HEADER_SIZE];
            System.arraycopy(bytes, HEADER_SIZE, nodeData, 0, nodeData.length);

            CRC32C crc = new CRC32C();
            crc.update(nodeData);
            if ((int) crc.getValue() != expectedCrc) return null;

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(nodeData));
            String[] patternIds = new String[nodeCount];
            int[][] neighbors = new int[nodeCount][];

            for (int i = 0; i < nodeCount; i++) {
                int idLen = in.readUnsignedShort();
                byte[] idBytes = new byte[idLen];
                in.readFully(idBytes);
                patternIds[i] = new String(idBytes, java.nio.charset.StandardCharsets.UTF_8);

                int nbrCount = in.readUnsignedShort();
                neighbors[i] = new int[nbrCount];
                for (int j = 0; j < nbrCount; j++) {
                    neighbors[i][j] = in.readInt();
                }
            }

            return new VamanaGraph(neighbors, patternIds, medoid, maxDegree);
        } catch (IOException e) {
            System.err.println("Failed to read Vamana graph from " + path + ": " + e.getMessage());
            return null;
        }
    }
}
