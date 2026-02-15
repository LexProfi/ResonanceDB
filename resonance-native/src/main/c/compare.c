/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
#ifdef __cplusplus
extern "C" {
#endif

#include <immintrin.h>
#include <math.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sleef.h>

#if defined(_WIN32) || defined(_WIN64)
  #define EXPORT __declspec(dllexport)
#else
  #define EXPORT __attribute__((visibility("default")))
#endif

#define MIN_ENERGY 1e-20f
#define MAX_LEN    (1u << 24)
#define MAX_COUNT  (1u << 24)

#ifndef DEBUG_MODE
  #define DEBUG_MODE 0
#endif

#ifndef M_PI
  #define M_PI 3.14159265358979323846
#endif

#ifndef USE_OMP
  #define OMP_FOR(x)
#else
  #include <omp.h>
  #define OMP_FOR(x) _Pragma(#x)
#endif

static inline float hsum256_ps(__m256 v) {
#if defined(__AVX__)
    __m128 vlow  = _mm256_castps256_ps128(v);
    __m128 vhigh = _mm256_extractf128_ps(v, 1);
    vlow = _mm_add_ps(vlow, vhigh);
    __m128 shuf = _mm_movehdup_ps(vlow);
    __m128 sums = _mm_add_ps(vlow, shuf);
    shuf = _mm_movehl_ps(shuf, sums);
    sums = _mm_add_ss(sums, shuf);
    return _mm_cvtss_f32(sums);
#else
    float tmp[8];
    _mm256_storeu_ps(tmp, v);
    return tmp[0]+tmp[1]+tmp[2]+tmp[3]+tmp[4]+tmp[5]+tmp[6]+tmp[7];
#endif
}

static inline float wrap_pi(float x) {
    const float pi = (float)M_PI;
    const float twoPi = 2.0f * (float)M_PI;
    while (x <= -pi) x += twoPi;
    while (x >  pi)  x -= twoPi;
    return x;
}

static float compare_scalar_cosdelta(const float *a1, const float *p1,
                                     const float *a2, const float *p2, int len) {
    float EA = 0.0f, EB = 0.0f, cross = 0.0f;
    for (int i = 0; i < len; ++i) {
        const float ai = a1[i], aj = a2[i];
        EA += ai * ai;
        EB += aj * aj;
        cross += ai * aj * cosf(p2[i] - p1[i]);
    }
    const float denom = EA + EB;
    if (denom <= MIN_ENERGY) return 0.0f;
    const float IF    = EA + EB + 2.0f * cross;
    const float base  = 0.5f * (IF / denom);
    const float ampF  = (EA > MIN_ENERGY && EB > MIN_ENERGY)
                        ? 2.0f * sqrtf(EA * EB) / denom : 0.0f;
    return base * ampF;
}

static void compare_with_delta_scalar(const float *A1, const float *P1,
                                      const float *A2, const float *P2,
                                      int len, float out[2]) {
    float EA = 0.0f, EB = 0.0f, cross = 0.0f, dsum = 0.0f;
    for (int i = 0; i < len; ++i) {
        const float a = A1[i], b = A2[i];
        const float d = P2[i] - P1[i];
        EA += a * a;
        EB += b * b;
        cross += a * b * cosf(d);
        dsum += wrap_pi(d);
    }
    const float denom = EA + EB;
    if (denom <= MIN_ENERGY) {
        out[0] = 0.0f; out[1] = 0.0f; return;
    }
    const float IF   = EA + EB + 2.0f * cross;
    const float ampF = (EA > MIN_ENERGY && EB > MIN_ENERGY)
                     ? 2.0f * sqrtf(EA * EB) / denom : 0.0f;
    out[0] = 0.5f * (IF / denom) * ampF;
    out[1] = dsum / (float)len;
}

EXPORT float compare_wave_patterns(const float *a1, const float *p1,
                                   const float *a2, const float *p2,
                                   int len)
{
    if (!a1 || !p1 || !a2 || !p2 || len <= 0 || len > (int)MAX_LEN) {
#if DEBUG_MODE
        fprintf(stderr, "[compare_wave_patterns] invalid args: a1=%p p1=%p a2=%p p2=%p len=%d\n",
                (void*)a1,(void*)p1,(void*)a2,(void*)p2,len);
#endif
        return 0.0f;
    }

#if defined(__AVX2__)
    const int step = 8;
    int i = 0;

    __m256 EA_v = _mm256_setzero_ps();
    __m256 EB_v = _mm256_setzero_ps();
    __m256 CR_v = _mm256_setzero_ps();

    for (; i <= len - step; i += step) {
        __m256 va1 = _mm256_loadu_ps(a1 + i);
        __m256 vp1 = _mm256_loadu_ps(p1 + i);
        __m256 va2 = _mm256_loadu_ps(a2 + i);
        __m256 vp2 = _mm256_loadu_ps(p2 + i);

        EA_v = _mm256_fmadd_ps(va1, va1, EA_v);
        EB_v = _mm256_fmadd_ps(va2, va2, EB_v);

        Sleef___m256_2 scQ = Sleef_sincosf8_u35avx2(vp1);
        Sleef___m256_2 sc2 = Sleef_sincosf8_u35avx2(vp2);
        __m256 s1 = scQ.x, c1 = scQ.y;
        __m256 s2 = sc2.x, c2 = sc2.y;

        __m256 term  = _mm256_fmadd_ps(c2, c1, _mm256_mul_ps(s2, s1)); // c2*c1 + s2*s1
        __m256 vA1A2 = _mm256_mul_ps(va1, va2);
        CR_v = _mm256_fmadd_ps(vA1A2, term, CR_v);
    }

    float EA = hsum256_ps(EA_v);
    float EB = hsum256_ps(EB_v);
    float cross = hsum256_ps(CR_v);

    for (; i < len; ++i) {
        const float ai = a1[i], aj = a2[i];
        EA += ai * ai;
        EB += aj * aj;
        cross += ai * aj * cosf(p2[i] - p1[i]);
    }

    const float denom = EA + EB;
    if (denom <= MIN_ENERGY) {
        _mm256_zeroupper();
        return 0.0f;
    }
    const float IF   = EA + EB + 2.0f * cross;
    const float base = 0.5f * (IF / denom);
    const float ampF = (EA > MIN_ENERGY && EB > MIN_ENERGY)
                     ? 2.0f * sqrtf(EA * EB) / denom : 0.0f;

    _mm256_zeroupper();
    return base * ampF;
#else
    return compare_scalar_cosdelta(a1, p1, a2, p2, len);
#endif
}

EXPORT void compare_many_flat(
    const float* restrict ampQ, const float* restrict phaseQ,
    const float* restrict ampAll, const float* restrict phaseAll,
    int len, int count, float* restrict out)
{
    if (!ampQ || !phaseQ || !ampAll || !phaseAll || !out ||
        len <= 0 || count <= 0 || len > (int)MAX_LEN || count > (int)MAX_COUNT) {
        return;
    }

#if defined(__AVX2__)
    const int step = 8;

    int i = 0;
    __m256 EA0=_mm256_setzero_ps(), EA1=_mm256_setzero_ps();
    for (; i <= len - 2*step; i += 2*step) {
        __m256 va0 = _mm256_loadu_ps(ampQ + i);
        __m256 va1 = _mm256_loadu_ps(ampQ + i + step);
        EA0 = _mm256_fmadd_ps(va0, va0, EA0);
        EA1 = _mm256_fmadd_ps(va1, va1, EA1);
    }
    __m256 EA_v = _mm256_add_ps(EA0, EA1);
    for (; i < len; ++i) {
        __m256 va = _mm256_set1_ps(ampQ[i]);
        EA_v = _mm256_fmadd_ps(va, va, EA_v);
    }
    float EA = hsum256_ps(EA_v);

    OMP_FOR(omp parallel for schedule(static) if (count >= 64))
    for (int k = 0; k < count; ++k) {
        const float* a2 = ampAll   + (size_t)k * len;
        const float* p2 = phaseAll + (size_t)k * len;

        int j = 0;
        __m256 EB0=_mm256_setzero_ps(), EB1=_mm256_setzero_ps();
        __m256 CR0=_mm256_setzero_ps(), CR1=_mm256_setzero_ps();

        for (; j <= len - 2*step; j += 2*step) {

            __m256 va1_0 = _mm256_loadu_ps(ampQ + j);
            __m256 vp1_0 = _mm256_loadu_ps(phaseQ + j);
            __m256 va2_0 = _mm256_loadu_ps(a2    + j);
            __m256 vp2_0 = _mm256_loadu_ps(p2    + j);

            Sleef___m256_2 scQ0 = Sleef_sincosf8_u35avx2(vp1_0);
            Sleef___m256_2 sc20 = Sleef_sincosf8_u35avx2(vp2_0);
            __m256 sQ0 = scQ0.x, cQ0 = scQ0.y;
            __m256 s20 = sc20.x, c20 = sc20.y;

            EB0 = _mm256_fmadd_ps(va2_0, va2_0, EB0);
            __m256 term0  = _mm256_fmadd_ps(c20, cQ0, _mm256_mul_ps(s20, sQ0));
            __m256 vA1A20 = _mm256_mul_ps(va1_0, va2_0);
            CR0 = _mm256_fmadd_ps(vA1A20, term0, CR0);

            __m256 va1_1 = _mm256_loadu_ps(ampQ + j + step);
            __m256 vp1_1 = _mm256_loadu_ps(phaseQ + j + step);
            __m256 va2_1 = _mm256_loadu_ps(a2    + j + step);
            __m256 vp2_1 = _mm256_loadu_ps(p2    + j + step);

            Sleef___m256_2 scQ1 = Sleef_sincosf8_u35avx2(vp1_1);
            Sleef___m256_2 sc21 = Sleef_sincosf8_u35avx2(vp2_1);
            __m256 sQ1 = scQ1.x, cQ1 = scQ1.y;
            __m256 s21 = sc21.x, c21 = sc21.y;

            EB1 = _mm256_fmadd_ps(va2_1, va2_1, EB1);
            __m256 term1  = _mm256_fmadd_ps(c21, cQ1, _mm256_mul_ps(s21, sQ1));
            __m256 vA1A21 = _mm256_mul_ps(va1_1, va2_1);
            CR1 = _mm256_fmadd_ps(vA1A21, term1, CR1);
        }

        __m256 EB_v = _mm256_add_ps(EB0, EB1);
        __m256 CR_v = _mm256_add_ps(CR0, CR1);

        for (; j < len; ++j) {
            const float a1j = ampQ[j], a2j = a2[j];
            EB_v = _mm256_add_ps(EB_v, _mm256_set1_ps(a2j*a2j));
            CR_v = _mm256_add_ps(CR_v, _mm256_set1_ps(a1j * a2j * cosf(p2[j] - phaseQ[j])));
        }

        float EB = hsum256_ps(EB_v);
        float cross = hsum256_ps(CR_v);

        const float denom = EA + EB;
        float score = 0.0f;
        if (denom > MIN_ENERGY) {
            const float IF   = EA + EB + 2.0f * cross;
            const float base = 0.5f * (IF / denom);
            const float ampF = (EA > MIN_ENERGY && EB > MIN_ENERGY)
                             ? 2.0f * sqrtf(EA * EB) / denom : 0.0f;
            score = base * ampF;
        }
        out[k] = score;
    }

    _mm256_zeroupper();
#else
    float EA = 0.0f;
    for (int i = 0; i < len; ++i) EA += ampQ[i]*ampQ[i];

    OMP_FOR(omp parallel for schedule(static) if (count >= 64))
    for (int k = 0; k < count; ++k) {
        const float* a2 = ampAll   + (size_t)k * len;
        const float* p2 = phaseAll + (size_t)k * len;
        float EB = 0.0f, cross = 0.0f;
        for (int j = 0; j < len; ++j) {
            const float a1j = ampQ[j], a2j = a2[j];
            EB    += a2j * a2j;
            cross += a1j * a2j * cosf(p2[j] - phaseQ[j]);
        }
        const float denom = EA + EB;
        float score = 0.0f;
        if (denom > MIN_ENERGY) {
            const float IF   = EA + EB + 2.0f * cross;
            const float base = 0.5f * (IF / denom);
            const float ampF = (EA > MIN_ENERGY && EB > MIN_ENERGY)
                             ? 2.0f * sqrtf(EA * EB) / denom : 0.0f;
            score = base * ampF;
        }
        out[k] = score;
    }
#endif
}

EXPORT void compare_many(
    const float* restrict ampQ, const float* restrict phaseQ,
    const float* restrict * ampList, const float* restrict * phaseList,
    int len, int count, float* restrict out)
{
    if (!ampQ || !phaseQ || !ampList || !phaseList || !out ||
        len <= 0 || count <= 0 || len > (int)MAX_LEN || count > (int)MAX_COUNT) {
        return;
    }

#if defined(__AVX2__)
    const int step = 8;

    int i = 0;
    __m256 EA0=_mm256_setzero_ps(), EA1=_mm256_setzero_ps();
    for (; i <= len - 2*step; i += 2*step) {
        __m256 va0 = _mm256_loadu_ps(ampQ + i);
        __m256 va1 = _mm256_loadu_ps(ampQ + i + step);
        EA0 = _mm256_fmadd_ps(va0, va0, EA0);
        EA1 = _mm256_fmadd_ps(va1, va1, EA1);
    }
    __m256 EA_v = _mm256_add_ps(EA0, EA1);
    for (; i < len; ++i) {
        __m256 va = _mm256_set1_ps(ampQ[i]);
        EA_v = _mm256_fmadd_ps(va, va, EA_v);
    }
    float EA = hsum256_ps(EA_v);

    OMP_FOR(omp parallel for schedule(static) if (count >= 64))
    for (int k = 0; k < count; ++k) {
        const float* a2 = ampList[k];
        const float* p2 = phaseList[k];
        if (!a2 || !p2) { out[k] = 0.0f; continue; }

        int j = 0;
        __m256 EB0=_mm256_setzero_ps(), EB1=_mm256_setzero_ps();
        __m256 CR0=_mm256_setzero_ps(), CR1=_mm256_setzero_ps();

        for (; j <= len - 2*step; j += 2*step) {
            __m256 va1_0 = _mm256_loadu_ps(ampQ + j);
            __m256 vp1_0 = _mm256_loadu_ps(phaseQ + j);
            __m256 va2_0 = _mm256_loadu_ps(a2    + j);
            __m256 vp2_0 = _mm256_loadu_ps(p2    + j);

            Sleef___m256_2 scQ0 = Sleef_sincosf8_u35avx2(vp1_0);
            Sleef___m256_2 sc20 = Sleef_sincosf8_u35avx2(vp2_0);
            __m256 sQ0 = scQ0.x, cQ0 = scQ0.y;
            __m256 s20 = sc20.x, c20 = sc20.y;

            EB0 = _mm256_fmadd_ps(va2_0, va2_0, EB0);
            __m256 term0  = _mm256_fmadd_ps(c20, cQ0, _mm256_mul_ps(s20, sQ0));
            __m256 vA1A20 = _mm256_mul_ps(va1_0, va2_0);
            CR0 = _mm256_fmadd_ps(vA1A20, term0, CR0);

            __m256 va1_1 = _mm256_loadu_ps(ampQ + j + step);
            __m256 vp1_1 = _mm256_loadu_ps(phaseQ + j + step);
            __m256 va2_1 = _mm256_loadu_ps(a2    + j + step);
            __m256 vp2_1 = _mm256_loadu_ps(p2    + j + step);

            Sleef___m256_2 scQ1 = Sleef_sincosf8_u35avx2(vp1_1);
            Sleef___m256_2 sc21 = Sleef_sincosf8_u35avx2(vp2_1);
            __m256 sQ1 = scQ1.x, cQ1 = scQ1.y;
            __m256 s21 = sc21.x, c21 = sc21.y;

            EB1 = _mm256_fmadd_ps(va2_1, va2_1, EB1);
            __m256 term1  = _mm256_fmadd_ps(c21, cQ1, _mm256_mul_ps(s21, sQ1));
            __m256 vA1A21 = _mm256_mul_ps(va1_1, va2_1);
            CR1 = _mm256_fmadd_ps(vA1A21, term1, CR1);
        }

        __m256 EB_v = _mm256_add_ps(EB0, EB1);
        __m256 CR_v = _mm256_add_ps(CR0, CR1);

        for (; j < len; ++j) {
            const float a1j = ampQ[j], a2j = a2[j];
            EB_v = _mm256_add_ps(EB_v, _mm256_set1_ps(a2j*a2j));
            CR_v = _mm256_add_ps(CR_v, _mm256_set1_ps(a1j * a2j * cosf(p2[j] - phaseQ[j])));
        }

        float EB = hsum256_ps(EB_v);
        float cross = hsum256_ps(CR_v);

        const float denom = EA + EB;
        float score = 0.0f;
        if (denom > MIN_ENERGY) {
            const float IF   = EA + EB + 2.0f * cross;
            const float base = 0.5f * (IF / denom);
            const float ampF = (EA > MIN_ENERGY && EB > MIN_ENERGY)
                             ? 2.0f * sqrtf(EA * EB) / denom : 0.0f;
            score = base * ampF;
        }
        out[k] = score;
    }

    _mm256_zeroupper();
#else
    float EA = 0.0f;
    for (int i = 0; i < len; ++i) EA += ampQ[i]*ampQ[i];

    OMP_FOR(omp parallel for schedule(static) if (count >= 64))
    for (int k = 0; k < count; ++k) {
        const float* a2 = ampList[k];
        const float* p2 = phaseList[k];
        if (!a2 || !p2) { out[k] = 0.0f; continue; }

        float EB = 0.0f, cross = 0.0f;
        for (int j = 0; j < len; ++j) {
            const float a1j = ampQ[j], a2j = a2[j];
            EB    += a2j * a2j;
            cross += a1j * a2j * cosf(p2[j] - phaseQ[j]);
        }

        const float denom = EA + EB;
        float score = 0.0f;
        if (denom > MIN_ENERGY) {
            const float IF   = EA + EB + 2.0f * cross;
            const float base = 0.5f * (IF / denom);
            const float ampF = (EA > MIN_ENERGY && EB > MIN_ENERGY)
                             ? 2.0f * sqrtf(EA * EB) / denom : 0.0f;
            score = base * ampF;
        }
        out[k] = score;
    }
#endif
}

EXPORT void compare_with_phase_delta(const float* restrict A1, const float* restrict P1,
                                     const float* restrict A2, const float* restrict P2,
                                     int len, float* restrict out)
{
    if (!out) {
#if DEBUG_MODE
        fprintf(stderr, "[compare_with_phase_delta] null out\n");
#endif
        return;
    }
    out[0] = out[1] = 0.0f;

    if (!A1 || !P1 || !A2 || !P2 || len <= 0 || len > (int)MAX_LEN) {
#if DEBUG_MODE
        fprintf(stderr, "[compare_with_phase_delta] invalid args\n");
#endif
        return;
    }

#if defined(__AVX2__)
    const int step = 8;
    int i = 0;

    __m256 EA0=_mm256_setzero_ps(), EA1=_mm256_setzero_ps();
    __m256 EB0=_mm256_setzero_ps(), EB1=_mm256_setzero_ps();
    __m256 CR0=_mm256_setzero_ps(), CR1=_mm256_setzero_ps();

    const __m256 twoPi = _mm256_set1_ps(2.0f * (float)M_PI);
    const __m256  pi   = _mm256_set1_ps((float)M_PI);
    const __m256 npi   = _mm256_set1_ps(-(float)M_PI);

    float dsum = 0.0f;

    for (; i <= len - 2*step; i += 2*step) {
        __m256 a1_0 = _mm256_loadu_ps(A1 + i);
        __m256 a2_0 = _mm256_loadu_ps(A2 + i);
        __m256 p1_0 = _mm256_loadu_ps(P1 + i);
        __m256 p2_0 = _mm256_loadu_ps(P2 + i);

        EA0 = _mm256_fmadd_ps(a1_0, a1_0, EA0);
        EB0 = _mm256_fmadd_ps(a2_0, a2_0, EB0);

        __m256 d0  = _mm256_sub_ps(p2_0, p1_0);
        __m256 dc0 = Sleef_cosf8_u10avx2(d0);
        __m256 a1a2_0 = _mm256_mul_ps(a1_0, a2_0);
        CR0 = _mm256_fmadd_ps(a1a2_0, dc0, CR0);

        __m256 gt0 = _mm256_cmp_ps(d0, pi , _CMP_GT_OS);
        __m256 lt0 = _mm256_cmp_ps(d0, npi, _CMP_LT_OS);
        d0 = _mm256_sub_ps(d0, _mm256_and_ps(gt0, twoPi));
        d0 = _mm256_add_ps(d0, _mm256_and_ps(lt0, twoPi));
        dsum += hsum256_ps(d0);

        __m256 a1_1 = _mm256_loadu_ps(A1 + i + step);
        __m256 a2_1 = _mm256_loadu_ps(A2 + i + step);
        __m256 p1_1 = _mm256_loadu_ps(P1 + i + step);
        __m256 p2_1 = _mm256_loadu_ps(P2 + i + step);

        EA1 = _mm256_fmadd_ps(a1_1, a1_1, EA1);
        EB1 = _mm256_fmadd_ps(a2_1, a2_1, EB1);

        __m256 d1  = _mm256_sub_ps(p2_1, p1_1);
        __m256 dc1 = Sleef_cosf8_u10avx2(d1);
        __m256 a1a2_1 = _mm256_mul_ps(a1_1, a2_1);
        CR1 = _mm256_fmadd_ps(a1a2_1, dc1, CR1);

        __m256 gt1 = _mm256_cmp_ps(d1, pi , _CMP_GT_OS);
        __m256 lt1 = _mm256_cmp_ps(d1, npi, _CMP_LT_OS);
        d1 = _mm256_sub_ps(d1, _mm256_and_ps(gt1, twoPi));
        d1 = _mm256_add_ps(d1, _mm256_and_ps(lt1, twoPi));
        dsum += hsum256_ps(d1);
    }

    __m256 EA_v = _mm256_add_ps(EA0, EA1);
    __m256 EB_v = _mm256_add_ps(EB0, EB1);
    __m256 CR_v = _mm256_add_ps(CR0, CR1);

    for (; i < len; ++i) {
        const float a = A1[i], b = A2[i];
        const float d = P2[i] - P1[i];
        EA_v = _mm256_add_ps(EA_v, _mm256_set1_ps(a*a));
        EB_v = _mm256_add_ps(EB_v, _mm256_set1_ps(b*b));
        CR_v = _mm256_add_ps(CR_v, _mm256_set1_ps(a * b * cosf(d)));
        dsum += wrap_pi(d);
    }

    float EA = hsum256_ps(EA_v);
    float EB = hsum256_ps(EB_v);
    float cross = hsum256_ps(CR_v);

    const float denom = EA + EB;
    if (denom > MIN_ENERGY) {
        const float IF   = EA + EB + 2.0f * cross;
        const float ampF = (EA > MIN_ENERGY && EB > MIN_ENERGY)
                         ? 2.0f * sqrtf(EA * EB) / denom : 0.0f;
        out[0] = 0.5f * (IF / denom) * ampF;
        out[1] = dsum / (float)len;
    } else {
        out[0] = 0.0f;
        out[1] = 0.0f;
    }

    _mm256_zeroupper();
#else
    compare_with_delta_scalar(A1, P1, A2, P2, len, out);
#endif
}

#ifdef __cplusplus
}
#endif
