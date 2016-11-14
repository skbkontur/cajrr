package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Ring {

    private static final Logger LOG = LoggerFactory.getLogger(Ring.class);

    Cluster cluster;

    private AtomicLong counter = new AtomicLong();
    private List<Token> tokens;

    public void fillRanges(List<String> lines) throws Exception {
        List<Range> ranges = new ArrayList<>(lines.size());
        for(String line: lines) {
            Range range = new Range(line);
            ranges.add(range);
        }
        Collections.sort(ranges, Range::compareTo);
        this.processRanges(ranges);

    }

    private void processRanges(List<Range> ranges) {
        Map<Long, String> map = new TreeMap<>();
        for (Range range: ranges) {
            map.put(range.start, range.endpoints);
        }
        processTokenMap(map);
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
    }

    public void processTokenMap(Map<Long, String> map) {

        Token first = null;
        Token prev = null;
        List<Token> result = Lists.newArrayList();

        for(Map.Entry<Long, String> entry: map.entrySet()) {
            try {
                String endpoints = entry.getValue();
                String key = entry.getKey().toString();
                Token token = new Token(key, endpoints, this);
                if(prev==null) {
                    first = token;
                    prev = token;
                    continue;
                }
                prev.setNext(token);
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
        fragment();
    }

    private void fragment() {
        counter.set(0);
        for(Token t: tokens) {
            t.fragment(slices, counter);
        }
    }

    public Boolean isPartitioner(String name) {
        return cluster.getPartitioner().endsWith(name);
    }
}
