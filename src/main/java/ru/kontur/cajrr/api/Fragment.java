package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;

class Fragment {
    private BigInteger start;
    private BigInteger end;

    Fragment(BigInteger start, BigInteger end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", start.toString(), end.subtract(BigInteger.ONE).toString());
    }

    @JsonProperty
    public String getStart() {
        return start.toString();
    }

    @JsonProperty
    public String getEnd() {
        return end.toString();
    }
}
