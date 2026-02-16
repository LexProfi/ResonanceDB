package ai.evacortex.resonancedb.rest.dto;

public final class WavePatternDto {

    public double[] amplitude;
    public double[] phase;

    public WavePatternDto() {
        // Required by Jackson
    }

    public WavePatternDto(double[] amplitude, double[] phase) {
        this.amplitude = amplitude;
        this.phase = phase;
    }
}