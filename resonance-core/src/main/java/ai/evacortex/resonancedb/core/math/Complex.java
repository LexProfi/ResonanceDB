/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025 Aleksandr Listopad
 * SPDX-License-Identifier: Prosperity-3.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.com
 */
package ai.evacortex.resonancedb.core.math;

/**
 * Immutable complex number used in WavePattern computation.
 * Optimized for numerical accuracy and compatibility with FFI.
 */
public final class Complex {

    public final double real;
    public final double imag;

    public Complex(double real, double imag) {
        this.real = real;
        this.imag = imag;
    }

    public Complex add(Complex other) {
        return new Complex(this.real + other.real, this.imag + other.imag);
    }

    public Complex subtract(Complex other) {
        return new Complex(this.real - other.real, this.imag - other.imag);
    }

    public Complex multiply(Complex other) {
        double r = this.real * other.real - this.imag * other.imag;
        double i = this.real * other.imag + this.imag * other.real;
        return new Complex(r, i);
    }

    public Complex scale(double factor) {
        return new Complex(this.real * factor, this.imag * factor);
    }

    public Complex conjugate() {
        return new Complex(this.real, -this.imag);
    }

    public double abs() {
        return Math.hypot(this.real, this.imag);
    }

    public double absSquared() {
        return this.real * this.real + this.imag * this.imag;
    }

    public Complex negate() {
        return new Complex(-this.real, -this.imag);
    }

    public double phase() {
        return Math.atan2(imag, real);
    }

    @Override
    public String toString() {
        return String.format("(%f %s %fi)", real, (imag < 0 ? "-" : "+"), Math.abs(imag));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Complex)) return false;
        Complex other = (Complex) obj;
        return Double.compare(real, other.real) == 0 && Double.compare(imag, other.imag) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(real) * 31 + Double.hashCode(imag);
    }
}