package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Ring {

    private static final Logger LOG = LoggerFactory.getLogger(Ring.class);

    Cluster cluster;

    private AtomicLong counter = new AtomicLong();
    private List<Token> tokens;

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
        Map<String, String> map = cluster.getTokenMap();
        Token first = null;
        Token prev = null;
        List<Token> result = Lists.newArrayList();

        for(Map.Entry<String, String> entry: map.entrySet()) {
            try {
                String endpoint = entry.getValue();
                Node node = cluster.findNode(endpoint);

                Token token = new Token(entry.getKey(), node, this);
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
