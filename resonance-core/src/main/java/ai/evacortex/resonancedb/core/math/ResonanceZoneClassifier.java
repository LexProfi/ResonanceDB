/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.math;

public final class ResonanceZoneClassifier {

    private static final float CORE_THRESHOLD = 0.85f;
    private static final float FRINGE_THRESHOLD = 0.3f;
    private static final double PHASE_LIMIT = Math.PI / 8;

    private ResonanceZoneClassifier() {}

    public static ResonanceZone classify(float energy, double phaseShift) {
        double absPhase = Math.abs(phaseShift % (2 * Math.PI));
        if (energy >= CORE_THRESHOLD && absPhase <= PHASE_LIMIT) {
            return ResonanceZone.CORE;
        } else if (energy >= FRINGE_THRESHOLD) {
            return ResonanceZone.FRINGE;
        } else {
            return ResonanceZone.SHADOW;
        }
    }

}
