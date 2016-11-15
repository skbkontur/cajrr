package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Ring {

    private static final Logger LOG = LoggerFactory.getLogger(Ring.class);

    Cluster cluster;

    private AtomicLong counter = new AtomicLong();
    private List<Token> tokens;

    public  BigInteger RANGE_MIN;

    public  BigInteger RANGE_MAX;
    public  BigInteger RANGE_SIZE;

    public void processRanges(List<String> lines) {
        List<Range> ranges = new ArrayList<>(lines.size());
        for(String line: lines) {
            Range range = new Range(line);
            ranges.add(range);
        }
        Collections.sort(ranges, Range::compareTo);

        processTokenMap(convertToMap(ranges));

    }

    private Map<BigInteger, String> convertToMap(List<Range> ranges) {
        Map<BigInteger, String> map = new TreeMap<>();
        for (Range range: ranges) {
            map.put(range.start, range.endpoints);
        }
        return map;
    }

    @JsonProperty
    public List<Token> getTokens() {
        return tokens;
    }

    @JsonProperty
    public int slices = 1;

    public Ring() {
        // Deserializer
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
        String partitioner = cluster.getPartitioner();
        if(partitioner.endsWith("RandomPartitioner")) {
            RANGE_MIN = BigInteger.ZERO;
            RANGE_MAX = new BigInteger("2").pow(127).subtract(BigInteger.ONE);
        } else if (partitioner.endsWith("Murmur3Partitioner")) {
            RANGE_MIN = new BigInteger("2").pow(63).negate();
            RANGE_MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
        }
        RANGE_SIZE = RANGE_MAX.subtract(RANGE_MIN).add(BigInteger.ONE);
    }

    public void processTokenMap(Map<BigInteger, String> map) {

        counter.set(0);
        Token first = null;
        Token prev = null;
        List<Token> result = Lists.newArrayList();

        for(Map.Entry<BigInteger, String> entry: map.entrySet()) {
            try {
                String endpoints = entry.getValue();
                Token token = new Token(entry.getKey(), endpoints, this);
                if(prev==null) {
                    first = token;
                    prev = token;
                    continue;
                }
                prev.setNext(token);
                prev.fragment(slices, counter);
                result.add(prev);
                prev = token;
            } catch (Exception e) {
                LOG.error(e.toString());
            }
        }
        assert prev != null;
        prev.setNext(first);
        result.add(prev);
        tokens = result;
    }

    public boolean isRandom = false;
    public boolean isMurmur = false;
    public boolean isRandomPartitioner() {
        return isRandom;
    }

    public boolean isMurmur3Partitioner() {
        return isMurmur;
    }
}
