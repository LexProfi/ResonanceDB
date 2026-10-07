/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.index;

import ai.evacortex.resonancedb.core.engine.PhaseWeights;
import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Per-centroid resonance moment statistics for weighted IVF routing.
 *
 * <p>For each centroid c and dimension i, stores:</p>
 * <ul>
 *   <li>μA[c][i] = mean(A[i]) — mean amplitude</li>
 *   <li>μR[c][i] = mean(A[i] * cos(φ[i])) — mean real projection</li>
 *   <li>μI[c][i] = mean(A[i] * sin(φ[i])) — mean imaginary projection</li>
 * </ul>
 * <p>Plus per-centroid scalars:</p>
 * <ul>
 *   <li>μE[c] = mean total energy of patterns in centroid c</li>
 *   <li>count[c] = number of patterns in centroid c</li>
 * </ul>
 *
 * <p>These are domain-neutral waveform statistics. Instances are immutable after construction.</p>
 */
public final class ResonanceMoments {

    private static final int MAGIC = 0x524D4F4D; // "RMOM"
    private static final int VERSION = 1;

    private final int centroidCount;
    private final int dimension;
    private final float[][] meanAmplitude;    // [centroid][dim]
    private final float[][] meanRealProj;     // [centroid][dim]
    private final float[][] meanImagProj;     // [centroid][dim]
    private final float[] meanEnergy;         // [centroid]
    private final int[] counts;               // [centroid]

    private ResonanceMoments(int centroidCount, int dimension,
                             float[][] meanAmplitude, float[][] meanRealProj,
                             float[][] meanImagProj, float[] meanEnergy, int[] counts) {
        this.centroidCount = centroidCount;
        this.dimension = dimension;
        this.meanAmplitude = meanAmplitude;
        this.meanRealProj = meanRealProj;
        this.meanImagProj = meanImagProj;
        this.meanEnergy = meanEnergy;
        this.counts = counts;
    }

    public int centroidCount() { return centroidCount; }
    public int dimension() { return dimension; }
    public float[] meanAmplitude(int centroid) { return meanAmplitude[centroid]; }
    public float[] meanRealProj(int centroid) { return meanRealProj[centroid]; }
    public float[] meanImagProj(int centroid) { return meanImagProj[centroid]; }
    public float meanEnergy(int centroid) { return meanEnergy[centroid]; }
    public int count(int centroid) { return counts[centroid]; }

    /**
     * Scores a centroid against a query using weighted phase participation.
     *
     * <p>Computes approximate cross-resonance:</p>
     * <pre>
     *   X = Σ Aq[i] * ((1 - w[i]) * μA[i] + w[i] * (cos(φq[i]) * μR[i] + sin(φq[i]) * μI[i]))
     * </pre>
     *
     * @return approximate resonance score for this centroid
     */
    public float scoreCentroid(int centroid, WavePattern query, PhaseWeights weights) {
        if (counts[centroid] == 0) return 0.0f;

        double[] qA = query.amplitude();
        double[] qP = query.phase();
        int D = qA.length;
        if (D != dimension) return 0.0f;

        float[] mA = meanAmplitude[centroid];
        float[] mR = meanRealProj[centroid];
        float[] mI = meanImagProj[centroid];

        double cross = 0.0;
        double queryEnergy = 0.0;

        if (weights == null || weights.isDefault()) {
            // Standard scoring: G = cos(Δφ), use μR and μI
            for (int i = 0; i < D; i++) {
                double Aq = qA[i];
                queryEnergy += Aq * Aq;
                double cosQ = Math.cos(qP[i]);
                double sinQ = Math.sin(qP[i]);
                cross += Aq * (cosQ * mR[i] + sinQ * mI[i]);
            }
        } else if (weights.isPhaseFree()) {
            // Amplitude-only: G = 1, cross = Aq * μA
            for (int i = 0; i < D; i++) {
                double Aq = qA[i];
                queryEnergy += Aq * Aq;
                cross += Aq * mA[i];
            }
        } else {
            // Weighted: G_i = (1 - w_i) + w_i * cos(Δφ_i)
            double[] w = weights.rawWeights();
            for (int i = 0; i < D; i++) {
                double Aq = qA[i];
                queryEnergy += Aq * Aq;
                double wi = w[i];
                double cosQ = Math.cos(qP[i]);
                double sinQ = Math.sin(qP[i]);
                double phaseTerm = cosQ * mR[i] + sinQ * mI[i];
                cross += Aq * ((1.0 - wi) * mA[i] + wi * phaseTerm);
            }
        }

        double Ec = meanEnergy[centroid];
        double Eq = queryEnergy;
        double denom = Eq + Ec;
        if (denom <= 0.0) return 0.0f;

        double base = 0.5 * (Eq + Ec + 2.0 * cross) / denom;
        double ampF = (Eq > 0.0 && Ec > 0.0) ? 2.0 * Math.sqrt(Eq * Ec) / denom : 0.0;
        return (float) Math.max(0.0, base * ampF);
    }

    /**
     * Ranks all centroids by approximate weighted resonance, returns top-K indices.
     */
    public int[] topCentroidsByMoment(WavePattern query, PhaseWeights weights, int topK) {
        int K = centroidCount;
        topK = Math.min(topK, K);

        float[] scores = new float[K];
        for (int c = 0; c < K; c++) {
            scores[c] = scoreCentroid(c, query, weights);
        }

        // Partial selection: find top-K by score descending
        int[] indices = new int[K];
        for (int i = 0; i < K; i++) indices[i] = i;

        for (int i = 0; i < topK; i++) {
            int best = i;
            for (int j = i + 1; j < K; j++) {
                if (scores[indices[j]] > scores[indices[best]]) {
                    best = j;
                }
            }
            if (best != i) {
                int tmp = indices[i];
                indices[i] = indices[best];
                indices[best] = tmp;
            }
        }

        int[] result = new int[topK];
        System.arraycopy(indices, 0, result, 0, topK);
        return result;
    }

    // ─── Builder ─────────────────────────────────────────────────────────────

    /**
     * Mutable accumulator for building moments incrementally during index construction.
     */
    public static final class Builder {
        private final int centroidCount;
        private final int dimension;
        private final double[][] sumA;
        private final double[][] sumR;
        private final double[][] sumI;
        private final double[] sumE;
        private final int[] counts;

        public Builder(int centroidCount, int dimension) {
            this.centroidCount = centroidCount;
            this.dimension = dimension;
            this.sumA = new double[centroidCount][dimension];
            this.sumR = new double[centroidCount][dimension];
            this.sumI = new double[centroidCount][dimension];
            this.sumE = new double[centroidCount];
            this.counts = new int[centroidCount];
        }

        public void addPattern(int centroid, WavePattern pattern) {
            double[] amp = pattern.amplitude();
            double[] phase = pattern.phase();
            int D = Math.min(amp.length, dimension);

            double energy = 0.0;
            for (int i = 0; i < D; i++) {
                double A = amp[i];
                double phi = phase[i];
                energy += A * A;
                sumA[centroid][i] += A;
                sumR[centroid][i] += A * Math.cos(phi);
                sumI[centroid][i] += A * Math.sin(phi);
            }
            sumE[centroid] += energy;
            counts[centroid]++;
        }

        public ResonanceMoments build() {
            float[][] mA = new float[centroidCount][dimension];
            float[][] mR = new float[centroidCount][dimension];
            float[][] mI = new float[centroidCount][dimension];
            float[] mE = new float[centroidCount];
            int[] c = new int[centroidCount];

            for (int k = 0; k < centroidCount; k++) {
                c[k] = counts[k];
                if (counts[k] > 0) {
                    double inv = 1.0 / counts[k];
                    mE[k] = (float) (sumE[k] * inv);
                    for (int i = 0; i < dimension; i++) {
                        mA[k][i] = (float) (sumA[k][i] * inv);
                        mR[k][i] = (float) (sumR[k][i] * inv);
                        mI[k][i] = (float) (sumI[k][i] * inv);
                    }
                }
            }

            return new ResonanceMoments(centroidCount, dimension, mA, mR, mI, mE, c);
        }
    }

    // ─── Persistence ─────────────────────────────────────────────────────────

    public void write(Path path) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tmp)))) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(centroidCount);
            out.writeInt(dimension);

            CRC32C crc = new CRC32C();
            byte[] buf = new byte[4];

            for (int c = 0; c < centroidCount; c++) {
                out.writeInt(counts[c]);
                writeFloatCrc(out, crc, buf, meanEnergy[c]);
                for (int i = 0; i < dimension; i++) writeFloatCrc(out, crc, buf, meanAmplitude[c][i]);
                for (int i = 0; i < dimension; i++) writeFloatCrc(out, crc, buf, meanRealProj[c][i]);
                for (int i = 0; i < dimension; i++) writeFloatCrc(out, crc, buf, meanImagProj[c][i]);
            }

            out.writeInt((int) crc.getValue());
        }
        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    public static ResonanceMoments load(Path path) {
        if (!Files.exists(path)) return null;

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            int magic = in.readInt();
            if (magic != MAGIC) return null;
            int version = in.readInt();
            if (version != VERSION) return null;

            int K = in.readInt();
            int D = in.readInt();
            if (K <= 0 || D <= 0) return null;

            float[][] mA = new float[K][D];
            float[][] mR = new float[K][D];
            float[][] mI = new float[K][D];
            float[] mE = new float[K];
            int[] counts = new int[K];

            CRC32C crc = new CRC32C();
            byte[] buf = new byte[4];

            for (int c = 0; c < K; c++) {
                counts[c] = in.readInt();
                mE[c] = readFloatCrc(in, crc, buf);
                for (int i = 0; i < D; i++) mA[c][i] = readFloatCrc(in, crc, buf);
                for (int i = 0; i < D; i++) mR[c][i] = readFloatCrc(in, crc, buf);
                for (int i = 0; i < D; i++) mI[c][i] = readFloatCrc(in, crc, buf);
            }

            int storedCrc = in.readInt();
            if (storedCrc != (int) crc.getValue()) {
                System.err.println("ResonanceMoments CRC mismatch, ignoring sidecar");
                return null;
            }

            return new ResonanceMoments(K, D, mA, mR, mI, mE, counts);
        } catch (IOException | RuntimeException e) {
            System.err.println("Failed to load ResonanceMoments: " + e.getMessage());
            return null;
        }
    }

    /**
     * Validates compatibility with the current centroid index.
     *
     * @return true if this moments object is compatible with the given index parameters
     */
    public boolean isCompatible(int expectedCentroidCount, int expectedDimension) {
        return centroidCount == expectedCentroidCount && dimension == expectedDimension;
    }

    private static void writeFloatCrc(DataOutputStream out, CRC32C crc, byte[] buf, float v) throws IOException {
        int bits = Float.floatToIntBits(v);
        buf[0] = (byte) (bits >>> 24);
        buf[1] = (byte) (bits >>> 16);
        buf[2] = (byte) (bits >>> 8);
        buf[3] = (byte) bits;
        crc.update(buf);
        out.writeFloat(v);
    }

    private static float readFloatCrc(DataInputStream in, CRC32C crc, byte[] buf) throws IOException {
        float v = in.readFloat();
        int bits = Float.floatToIntBits(v);
        buf[0] = (byte) (bits >>> 24);
        buf[1] = (byte) (bits >>> 16);
        buf[2] = (byte) (bits >>> 8);
        buf[3] = (byte) bits;
        crc.update(buf);
        return v;
    }
}
