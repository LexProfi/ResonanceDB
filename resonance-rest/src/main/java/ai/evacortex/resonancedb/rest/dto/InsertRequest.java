package ai.evacortex.resonancedb.rest.dto;

import java.util.Map;

public record InsertRequest(
        WavePatternDto pattern,
        Map<String, String> metadata
) {}