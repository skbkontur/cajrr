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

    @JsonProperty
    public String getMax() {
        return RANGE_MAX.toString();
    }

    Token(Ring ring, String key) throws Exception {
        this.key = key;
        if (ring.partitioner.endsWith("RandomPartitioner")) {
            RANGE_MIN = BigInteger.ZERO;
            RANGE_MAX = new BigInteger("2").pow(127).subtract(BigInteger.ONE);
        } else if (ring.partitioner.endsWith("Murmur3Partitioner")) {
            RANGE_MIN = new BigInteger("2").pow(63).negate();
            RANGE_MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
        } else {
            throw new Exception("Unsupported partitioner " + ring.partitioner);
        }
        RANGE_SIZE = RANGE_MAX.subtract(RANGE_MIN).add(BigInteger.ONE);

    }

    private static boolean greaterThan(BigInteger a, BigInteger b) {
        return a.compareTo(b) > 0;
    }

    void setNext(Token next) {
        this.next = next;
    }

    List<Fragment> fragment(long slices, AtomicLong counter) {
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
            result.add(new Fragment(counter.incrementAndGet(), endpointTokens.get(j), endpointTokens.get(j + 1)));
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
