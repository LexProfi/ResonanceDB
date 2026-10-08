/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.bench;

import ai.evacortex.resonancedb.core.storage.WavePattern;

import java.util.Random;

/**
 * Generates a synthetic structured waveform corpus with realistic cluster properties.
 *
 * <p>Design goals:</p>
 * <ul>
 *   <li>Overlapping clusters — some cluster centers are placed close together</li>
 *   <li>Variable cluster sizes — log-normal distribution of pattern counts</li>
 *   <li>Variable cluster densities — per-cluster sigma for amplitude and phase</li>
 *   <li>Noise dimensions — a fraction of dimensions carry no cluster signal</li>
 *   <li>Intra-cluster phase and amplitude variation</li>
 *   <li>Partially overlapping phase distributions between neighboring clusters</li>
 * </ul>
 *
 * <p>Cluster labels are recorded for analysis only.
 * They must NOT be used to construct PhaseWeights.</p>
 */
final class StructuredCorpusGenerator {

    /** Configuration for the structured corpus generator. */
    record Config(
            int numClusters,
            int dimension,
            int totalPatterns,
            double noiseDimFraction,
            double overlapFraction,
            double ampSigmaBase,
            double phaseSigmaBase,
            long seed
    ) {
        static Config standard(int dim, int n, long seed) {
            int k = Math.max(8, (int) Math.ceil(Math.sqrt(n) * 0.6));
            return new Config(k, dim, n, 0.25, 0.30, 0.12, 0.35, seed);
        }
    }

    /** Generated corpus with patterns, labels, and metadata. */
    record Corpus(
            WavePattern[] patterns,
            int[] clusterLabels,
            int numClusters,
            int[] clusterSizes,
            Config config
    ) {
        @Override
        public String toString() {
            return String.format("StructuredCorpus[N=%d, D=%d, K=%d, noiseFrac=%.0f%%, overlapFrac=%.0f%%]",
                    patterns.length, config.dimension(), numClusters,
                    config.noiseDimFraction() * 100, config.overlapFraction() * 100);
        }
    }

    static Corpus generate(Config config) {
        Random rng = new Random(config.seed());
        int D = config.dimension();
        int K = config.numClusters();
        int N = config.totalPatterns();

        int[] clusterSizes = generateClusterSizes(K, N, rng);

        int noiseDims = (int) (D * config.noiseDimFraction());
        boolean[] isNoise = selectNoiseDimensions(D, noiseDims, rng);

        double[][] centerAmp = new double[K][D];
        double[][] centerPhase = new double[K][D];
        double[] sigmaAmp = new double[K];
        double[] sigmaPhase = new double[K];

        for (int k = 0; k < K; k++) {
            sigmaAmp[k] = config.ampSigmaBase() * (0.5 + rng.nextDouble() * 1.5);
            sigmaPhase[k] = config.phaseSigmaBase() * (0.5 + rng.nextDouble() * 1.5);

            for (int d = 0; d < D; d++) {
                if (isNoise[d]) {
                    centerAmp[k][d] = 0.3 + rng.nextDouble() * 0.5;
                    centerPhase[k][d] = rng.nextDouble() * 2 * Math.PI;
                } else {
                    centerAmp[k][d] = 0.1 + rng.nextDouble() * 0.9;
                    centerPhase[k][d] = rng.nextDouble() * 2 * Math.PI;
                }
            }
        }

        int overlapCount = (int) (K * config.overlapFraction());
        for (int i = 0; i < overlapCount; i++) {
            int source = rng.nextInt(K);
            int target = (source + 1 + rng.nextInt(Math.max(1, K - 1))) % K;
            double proximity = 0.05 + rng.nextDouble() * 0.15;
            for (int d = 0; d < D; d++) {
                if (!isNoise[d]) {
                    centerAmp[target][d] = centerAmp[source][d]
                            + (centerAmp[target][d] - centerAmp[source][d]) * proximity
                            + rng.nextGaussian() * 0.05;
                    centerAmp[target][d] = Math.max(0.01, centerAmp[target][d]);

                    double phaseDiff = centerPhase[target][d] - centerPhase[source][d];
                    centerPhase[target][d] = centerPhase[source][d]
                            + phaseDiff * proximity
                            + rng.nextGaussian() * 0.2;
                }
            }
            sigmaAmp[target] *= 0.7;
            sigmaPhase[target] *= 0.7;
        }

        WavePattern[] patterns = new WavePattern[N];
        int[] labels = new int[N];
        int idx = 0;

        for (int k = 0; k < K; k++) {
            for (int j = 0; j < clusterSizes[k]; j++) {
                double[] amp = new double[D];
                double[] phase = new double[D];

                for (int d = 0; d < D; d++) {
                    if (isNoise[d]) {
                        amp[d] = Math.max(0.001, 0.4 + rng.nextGaussian() * 0.15);
                        phase[d] = rng.nextDouble() * 2 * Math.PI;
                    } else {
                        amp[d] = Math.max(0.001,
                                centerAmp[k][d] + rng.nextGaussian() * sigmaAmp[k]);
                        phase[d] = centerPhase[k][d] + rng.nextGaussian() * sigmaPhase[k];
                    }
                    phase[d] = ((phase[d] % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI);
                }

                patterns[idx] = new WavePattern(amp, phase);
                labels[idx] = k;
                idx++;
            }
        }

        for (int i = N - 1; i > 0; i--) {
            int j = rng.nextInt(i + 1);
            WavePattern tmp = patterns[i];
            patterns[i] = patterns[j];
            patterns[j] = tmp;
            int tmpL = labels[i];
            labels[i] = labels[j];
            labels[j] = tmpL;
        }

        return new Corpus(patterns, labels, K, clusterSizes, config);
    }

    private static int[] generateClusterSizes(int K, int N, Random rng) {
        double[] rawSizes = new double[K];
        for (int i = 0; i < K; i++) {
            rawSizes[i] = Math.exp(rng.nextGaussian() * 0.8 + 1.0);
        }
        double sizeSum = 0;
        for (double s : rawSizes) {
            sizeSum += s;
        }

        int[] sizes = new int[K];
        int assigned = 0;
        for (int i = 0; i < K - 1; i++) {
            sizes[i] = Math.max(2, (int) Math.round(N * rawSizes[i] / sizeSum));
            assigned += sizes[i];
        }
        sizes[K - 1] = Math.max(2, N - assigned);

        int total = 0;
        for (int s : sizes) {
            total += s;
        }
        if (total != N) {
            int maxIdx = 0;
            for (int i = 1; i < K; i++) {
                if (sizes[i] > sizes[maxIdx]) maxIdx = i;
            }
            sizes[maxIdx] += N - total;
        }

        return sizes;
    }

    private static boolean[] selectNoiseDimensions(int D, int noiseDims, Random rng) {
        boolean[] isNoise = new boolean[D];
        int[] indices = new int[D];
        for (int i = 0; i < D; i++) indices[i] = i;
        for (int i = 0; i < noiseDims && i < D; i++) {
            int j = i + rng.nextInt(D - i);
            int tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
            isNoise[indices[i]] = true;
        }
        return isNoise;
    }

    /** Generate a random (unstructured) corpus as negative control. */
    static Corpus generateRandom(int dim, int n, long seed) {
        Random rng = new Random(seed);
        WavePattern[] patterns = new WavePattern[n];
        int[] labels = new int[n];
        for (int i = 0; i < n; i++) {
            double[] amp = new double[dim];
            double[] phase = new double[dim];
            for (int d = 0; d < dim; d++) {
                amp[d] = rng.nextDouble();
                phase[d] = rng.nextDouble() * 2 * Math.PI;
            }
            patterns[i] = new WavePattern(amp, phase);
        }
        return new Corpus(patterns, labels, 1, new int[]{n},
                new Config(1, dim, n, 1.0, 0.0, 0.0, 0.0, seed));
    }

    private StructuredCorpusGenerator() {}
}
