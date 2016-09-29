package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
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
}
