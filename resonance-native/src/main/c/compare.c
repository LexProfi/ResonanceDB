/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
#include <math.h>
#include <immintrin.h>
#include <stddef.h>

/**
 * Compare two complex wave patterns using AVX2.
 */
EXPORT float compare_wave_patterns(const float* amp1, const float* phase1,
                                   const float* amp2, const float* phase2,
                                   int len) {
    float energyA = 0.0f;
    float energyB = 0.0f;
    float interference = 0.0f;

    int i = 0;
    const int step = 8;

    for (; i <= len - step; i += step) {
        __m256 a1 = _mm256_loadu_ps(&amp1[i]);
        __m256 p1 = _mm256_loadu_ps(&phase1[i]);
        __m256 a2 = _mm256_loadu_ps(&amp2[i]);
        __m256 p2 = _mm256_loadu_ps(&phase2[i]);

        float p1_vals[8], p2_vals[8];
        _mm256_storeu_ps(p1_vals, p1);
        _mm256_storeu_ps(p2_vals, p2);

        float cos1[8], sin1[8], cos2[8], sin2[8];
        for (int j = 0; j < 8; ++j) {
            sincosf(p1_vals[j], &sin1[j], &cos1[j]);
            sincosf(p2_vals[j], &sin2[j], &cos2[j]);
        }

        __m256 cs1 = _mm256_loadu_ps(cos1);
        __m256 sn1 = _mm256_loadu_ps(sin1);
        __m256 cs2 = _mm256_loadu_ps(cos2);
        __m256 sn2 = _mm256_loadu_ps(sin2);

        __m256 r1 = _mm256_mul_ps(a1, cs1);
        __m256 i1 = _mm256_mul_ps(a1, sn1);
        __m256 r2 = _mm256_mul_ps(a2, cs2);
        __m256 i2 = _mm256_mul_ps(a2, sn2);

        __m256 re = _mm256_add_ps(r1, r2);
        __m256 im = _mm256_add_ps(i1, i2);

        __m256 re2 = _mm256_mul_ps(re, re);
        __m256 im2 = _mm256_mul_ps(im, im);
        __m256 sumInterf = _mm256_add_ps(re2, im2);

        __m256 r1_2 = _mm256_mul_ps(r1, r1);
        __m256 i1_2 = _mm256_mul_ps(i1, i1);
        __m256 r2_2 = _mm256_mul_ps(r2, r2);
        __m256 i2_2 = _mm256_mul_ps(i2, i2);

        __m256 eA = _mm256_add_ps(r1_2, i1_2);
        __m256 eB = _mm256_add_ps(r2_2, i2_2);

        float tmp[8];

        _mm256_storeu_ps(tmp, sumInterf);
        for (int j = 0; j < 8; ++j) interference += tmp[j];

        _mm256_storeu_ps(tmp, eA);
        for (int j = 0; j < 8; ++j) energyA += tmp[j];

        _mm256_storeu_ps(tmp, eB);
        for (int j = 0; j < 8; ++j) energyB += tmp[j];
    }

    // Scalar fallback for remaining elements
    for (; i < len; ++i) {
        float r1 = amp1[i] * cosf(phase1[i]);
        float i1 = amp1[i] * sinf(phase1[i]);
        float r2 = amp2[i] * cosf(phase2[i]);
        float i2 = amp2[i] * sinf(phase2[i]);

        float re = r1 + r2;
        float im = i1 + i2;

        interference += re * re + im * im;
        energyA += r1 * r1 + i1 * i1;
        energyB += r2 * r2 + i2 * i2;
    }

    float energySum = energyA + energyB;
    if (energySum == 0.0f) return 0.0f;

    float base = 0.5f * interference / energySum;
    float ampFactor = 0.0f;

    if (energyA > 0.0f && energyB > 0.0f) {
        ampFactor = 2.0f * sqrtf(energyA * energyB) / energySum;
    }

    return base * ampFactor;
}

/**
 * Batched comparison: computes similarity scores between query pattern and N candidate patterns.
 */
EXPORT void compare_many(const float* ampQ, const float* phaseQ,
                         const float** ampList, const float** phaseList,
                         int len, int count, float* out) {
    for (int i = 0; i < count; ++i) {
        out[i] = compare_wave_patterns(ampQ, phaseQ, ampList[i], phaseList[i], len);
    }
}