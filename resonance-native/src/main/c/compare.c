#include <math.h>
#include <immintrin.h> // AVX2
#include <stddef.h>

float compare_wave_patterns(const float* amp1, const float* phase1,
                            const float* amp2, const float* phase2,
                            int len) {
    float energyA = 0.0f;
    float energyB = 0.0f;
    float interference = 0.0f;

    for (int i = 0; i < len; i++) {
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

    float denom = energyA + energyB;
    return denom == 0.0f ? 0.0f : interference / denom;
}
