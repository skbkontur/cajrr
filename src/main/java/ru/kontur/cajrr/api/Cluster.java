package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.jersey.params.IntParam;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.tools.RepairObserver;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class Cluster {
    private boolean connected = false;

    @JsonProperty
    public String name;

    @JsonProperty
    public List<Node> nodes;

    private Map<Long, Repair> activeRepairs = new HashMap<>();

    private static final Logger LOG = LoggerFactory.getLogger(Repair.class);

    public void connect() {
        try {
            for(Node node: nodes) {
                node.connect();
            }
            connected = true;
        } catch (IOException e) {
            connected = false;
            e.printStackTrace();
        }
    }

    private Map<BigInteger,String> getTokenMap() {
        Map<String, String> map = defaultNode().getTokenToEndpointMap();
        Map<BigInteger, String> result = new TreeMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            BigInteger key = new BigInteger(entry.getKey());
            String value = entry.getValue();
            result.put(key, value);
        }
        return result;
    }

    private Node defaultNode() {
        return nodes.get(0);
    }

    String getPartitioner() {
        return defaultNode().getPartitioner();
    }

    private Node findNode(String endpoint) {

        for(Node node: nodes) {
            if (node.getHost().equals(endpoint)) {
                return node;
            }
        }
        return defaultNode();
    }

    public boolean isConnected() {
        return connected;
    }

    public void registerRepair(Repair repair) throws Exception {
        activeRepairs.put(repair.id, repair);
        Node proxy = findNode(repair.GetProxyNode());
        RepairObserver observer = new RepairObserver(repair, proxy);
        try {
            proxy.addListener(observer);
            observer.run();
        }   catch (Exception e) {
            LOG.error(e.getMessage());
            repair.progress(new ProgressEvent(ProgressEventType.ERROR, 0, 0, e.getMessage()), false);
        }
    }

    public List<Token> describeRing(String keyspace, int slices) {
        Ring ring = new Ring(this, slices);
        List<String> ranges = defaultNode().describeRing(keyspace);

        return ring.getTokensFromRanges(ranges);

    }

    public List<Token> getRing() {
        Ring ring = new Ring(this, 1);
        Map<BigInteger, String> map = this.getTokenMap();

        return ring.getTokensFromMap(map);
    }
}
