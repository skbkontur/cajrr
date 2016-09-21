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
        return String.format("%s:%s", getStart().toString(), getEnd().subtract(BigInteger.ONE).toString());
    }

    @JsonProperty
    public BigInteger getStart() {
        return start;
    }

    @JsonProperty
    public BigInteger getEnd() {
        return end;
    }
}
