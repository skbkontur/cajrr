package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.tools.RepairObserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cluster {
    private boolean connected = false;

    @JsonProperty
    public String name;

    @JsonProperty
    public Ring ring;

    @JsonProperty
    public List<Node> nodes;

    private Map<Long, Repair> activeRepairs = new HashMap<>();

    private static final Logger LOG = LoggerFactory.getLogger(Repair.class);

    public void connect() {
        try {
            for(Node node: nodes) {
                node.connect();
            }
            ring.setCluster(this);
            connected = true;
        } catch (IOException e) {
            connected = false;
            e.printStackTrace();
        }
    }

    Map<String,String> getTokenMap() {
        return defaultNode().getTokenToEndpointMap();
    }

    private Node defaultNode() {
        return nodes.get(0);
    }

    String getPartitioner() {
        return defaultNode().getPartitioner();
    }

    Node findNode(String endpoint) {

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
        Node proxy = findNode(repair.fragment.endpoint);
        RepairObserver observer = new RepairObserver(repair, proxy);
        try {
            proxy.addListener(observer);
            observer.run();
        }   catch (Exception e) {
            LOG.error(e.getMessage());
            repair.progress(new ProgressEvent(ProgressEventType.ERROR, 0, 0, e.getMessage()), false);
        }
    }

}
