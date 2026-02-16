package ai.evacortex.resonancedb.rest.dto;

import java.util.Map;

public record ReplaceRequest(
        String id,
        WavePatternDto pattern,
        Map<String, String> metadata
) {}