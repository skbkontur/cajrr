package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;

class Fragment {

    @JsonProperty
    public long id;

    @JsonProperty
    public String endpoint;

    private BigInteger start;
    private BigInteger end;


    public Fragment() {
        // JSON
    }

    Fragment(long id, BigInteger start, BigInteger end) {
        this.id = id;
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

    @JsonProperty
    public void setStart(String start) {
        this.start = new BigInteger(start);
    }

    @JsonProperty
    public void setEnd(String end) {
        this.end = new BigInteger(end);
    }
}
