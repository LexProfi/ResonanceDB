/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.rest.validation;

import ai.evacortex.resonancedb.core.storage.WavePattern;
import ai.evacortex.resonancedb.rest.ResonanceRestConfig;
import ai.evacortex.resonancedb.rest.dto.WavePatternDto;
import ai.evacortex.resonancedb.rest.error.BadRequestException;

import java.util.Objects;

/**
 * Validates and converts REST WavePatternDto to core {@link WavePattern}.
 *
 * Contract-critical:
 *  - Must preserve original DTO validation messages and semantics:
 *      null amplitude/phase -> "WavePattern requires 'amplitude' and 'phase'"
 *      length mismatch -> "WavePattern amplitude/phase length mismatch"
 *      empty length -> "WavePattern length must be > 0"
 *
 * Optional (disabled by default):
 *  - validateFiniteWaveValues: if enabled, rejects NaN/Infinity with BadRequestException.
 */
public final class WavePatternValidator {

    private final ResonanceRestConfig cfg;

    public WavePatternValidator(ResonanceRestConfig cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    public WavePattern toWavePattern(WavePatternDto dto) {
        if (dto == null) {
            // Previously Jackson would NPE later; keep it as a clean 400 bad_request.
            throw new BadRequestException("WavePattern is required");
        }

        double[] amplitude = dto.amplitude;
        double[] phase = dto.phase;

        if (amplitude == null || phase == null) {
            throw new BadRequestException("WavePattern requires 'amplitude' and 'phase'");
        }
        if (amplitude.length != phase.length) {
            throw new BadRequestException("WavePattern amplitude/phase length mismatch");
        }
        if (amplitude.length == 0) {
            throw new BadRequestException("WavePattern length must be > 0");
        }

        if (cfg.validateFiniteWaveValues()) {
            validateFinite(amplitude, "amplitude");
            validateFinite(phase, "phase");
        }

        // Preserve original behavior: pass arrays as-is (no copy) to avoid extra allocations.
        return new WavePattern(amplitude, phase);
    }

    private static void validateFinite(double[] v, String name) {
        for (int i = 0; i < v.length; i++) {
            double x = v[i];
            if (!Double.isFinite(x)) {
                throw new BadRequestException("WavePattern " + name + " contains non-finite value at index " + i);
            }
        }
    }
}