package ru.kontur.cajrr.api;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Ring {

    private static final Logger LOG = LoggerFactory.getLogger(Ring.class);

    private AtomicLong counter = new AtomicLong();

    BigInteger RANGE_MIN;

    BigInteger RANGE_MAX;
    BigInteger RANGE_SIZE;

    public List<Token> getTokensFromRanges(List<String> lines) {
        List<Range> ranges = new ArrayList<>();
        for(String line: lines) {
            Range range = new Range(line);
            ranges.add(range);
        }
        Collections.sort(ranges, Range::compareTo);

        return getTokensFromMap(convertToMap(ranges));

    }

    private Map<BigInteger, String> convertToMap(List<Range> ranges) {
        Map<BigInteger, String> map = new TreeMap<>();
        for (Range range: ranges) map.put(range.start, range.endpoints);
        return map;
    }

    int slices = 1;

    public Ring(String partitioner, int slices) {
        this.slices = slices;
        if(partitioner.endsWith("RandomPartitioner")) {
            RANGE_MIN = BigInteger.ZERO;
            RANGE_MAX = new BigInteger("2").pow(127).subtract(BigInteger.ONE);
        } else if (partitioner.endsWith("Murmur3Partitioner")) {
            RANGE_MIN = new BigInteger("2").pow(63).negate();
            RANGE_MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
        }
        RANGE_SIZE = RANGE_MAX.subtract(RANGE_MIN).add(BigInteger.ONE);
    }

    public List<Token> getTokensFromMap(Map<BigInteger, String> map) {

        counter.set(0);
        Token first = null;
        Token prev = null;
        List<Token> result = Lists.newArrayList();

        for(Map.Entry<BigInteger, String> entry: map.entrySet()) {
            try {
                String endpoints = entry.getValue();
                Token token = new Token(entry.getKey(), this);
                token.endpoint = endpoints;
                if (prev == null) {
                    first = token;
                    prev = token;
                    continue;
                }
                prev.setNext(token);
                prev.fragment(slices, counter);
                result.add(prev);
                prev = token;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        assert prev != null;
        prev.setNext(first);
        prev.fragment(slices, counter);
        result.add(prev);
        return result;
    }
}
