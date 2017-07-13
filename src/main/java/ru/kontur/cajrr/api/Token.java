package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Token {

    private final BigInteger RANGE_MIN;
    private final BigInteger RANGE_MAX;
    private final BigInteger RANGE_SIZE;

    @JsonProperty
    public String endpoint;
    @JsonProperty
    public List<Fragment> fragments;

    public String getStart() {
        return start.toString();
    }

    private BigInteger start;
    private BigInteger size;

    public Token(BigInteger key, BigInteger rangeMin, BigInteger rangeMax, BigInteger rangeSize) {
        this.start = key;
        RANGE_MIN = rangeMin;
        RANGE_MAX = rangeMax;
        RANGE_SIZE = rangeSize;
    }

    public void setNext(Token next) {
        BigInteger stop = next.start;
        size = stop.subtract(start);

        if (start.compareTo(stop)>0) {
            size = RANGE_MAX.subtract(start).add(stop.subtract(RANGE_MIN)).add(BigInteger.ONE);
        }
    }

    public List<Fragment> fragment(int slices, AtomicLong counter) {
        List<Fragment> result = Lists.newArrayList();


        BigInteger[] segmentCountAndRemainder =
                size.multiply(BigInteger.valueOf(slices)).divideAndRemainder(size);
        int segmentCount = segmentCountAndRemainder[0].intValue() +
                (segmentCountAndRemainder[1].equals(BigInteger.ZERO) ? 0 : 1);

        List<BigInteger> endpointTokens = Lists.newArrayList();
        for (int j = 0; j <= segmentCount; j++) {
            BigInteger offset = size
                    .multiply(BigInteger.valueOf(j))
                    .divide(BigInteger.valueOf(segmentCount));
            BigInteger nextToken = start.add(offset);
            // Bordered values from MAX to MIN
            if (nextToken.compareTo(RANGE_MAX)>0) {
                nextToken = nextToken.subtract(RANGE_SIZE);
            }
            endpointTokens.add(nextToken);
        }

        for (int j = 0; j < segmentCount; j++) {
            Fragment frag = new Fragment(counter.incrementAndGet(), endpointTokens.get(j), endpointTokens.get(j + 1));
            frag.endpoint = this.endpoint;
            result.add(frag);
        }
        fragments = result;
        return result;
    }

}
