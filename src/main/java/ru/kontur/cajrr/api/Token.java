package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

class Token {

    private final BigInteger RANGE_MIN;
    private final BigInteger RANGE_MAX;
    private final BigInteger RANGE_SIZE;
    private String key;
    private Token next;
    private List<Fragment> ranges;


    Token(String key) {
        this.key = key;

        RANGE_MIN = new BigInteger("2").pow(63).negate();
        RANGE_MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
        RANGE_SIZE = RANGE_MAX.subtract(RANGE_MIN).add(BigInteger.ONE);

    }

    private static boolean greaterThan(BigInteger a, BigInteger b) {
        return a.compareTo(b) > 0;
    }

    void setNext(Token next) {
        this.next = next;
    }

    List<Fragment> fragment(int slices, AtomicLong counter) {
        List<Fragment> result = Lists.newArrayList();
        BigInteger start = value();
        BigInteger stop = next.value();

        BigInteger size = stop.subtract(start);

        if (greaterThan(start, stop)) {
            size = RANGE_MAX.subtract(start).add(stop.subtract(RANGE_MIN)).add(BigInteger.ONE);
        }

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
            if (greaterThan(nextToken, RANGE_MAX)) {
                nextToken = nextToken.subtract(RANGE_SIZE);
            }
            endpointTokens.add(nextToken);
        }

        for (int j = 0; j < segmentCount; j++) {
            result.add(new Fragment(counter.incrementAndGet(), endpointTokens.get(j), endpointTokens.get(j + 1).subtract(BigInteger.ONE)));
        }
        ranges = result;
        return result;
    }

    private BigInteger value() {
        return new BigInteger(key);
    }

    @JsonProperty
    public List<Fragment> getRanges() {
        return ranges;
    }

    @JsonProperty
    public String getKey() {
        return key;
    }
}