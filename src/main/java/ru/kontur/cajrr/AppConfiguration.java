package ru.kontur.cajrr;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import ru.kontur.cajrr.api.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AppConfiguration extends Configuration {

    @JsonProperty
    public int combinationThreshold = 10000;

    @JsonProperty
    public int minSlicingSize = 10000000;

    @JsonProperty
    public int maxSlices = 50;


    @JsonProperty
    public boolean connected = false;

    @JsonProperty
    public String cluster;

    @JsonProperty
    public String callback = "localhost:8888";

    @JsonProperty
    public List<Node> nodes = new ArrayList<>();

    public Node findNode(String endpoint) {

        for(Node node: nodes) {
            if (node.getHost().equals(endpoint)) {
                return node;
            }
        }
        return defaultNode();
    }
    public Node defaultNode() {
        return nodes.get(0);
    }

    public boolean isConnected() {
        return connected;
    }


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
}
