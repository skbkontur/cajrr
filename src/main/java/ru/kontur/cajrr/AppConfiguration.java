package ru.kontur.cajrr;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import ru.kontur.cajrr.api.Elasticsearch;
import ru.kontur.cajrr.api.Node;

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
    public int interval = 60*60*24*7;

    @JsonProperty
    public String cluster;

    @JsonProperty
    public List<Node> nodes = new ArrayList<>();

    @JsonProperty
    public List<String> keyspaces = new ArrayList<>();

    @JsonProperty
    public List<String> exclude = new ArrayList<>();


    @JsonProperty
    public Elasticsearch elastic;

    @JsonProperty
    public String serviceName = "cajrr";

    @JsonProperty
    public String serviceHost = "localhost";

    @JsonProperty
    public Integer servicePort = 8080;

    @JsonProperty
    public String consul = "localhost:8500";

    public Node findNode(String endpoint) {

        for(Node node: nodes) {
            String host = node.getHost();
            if (host.equals(endpoint)) {
                return node;
            }
        }
        return defaultNode();
    }
    public Node defaultNode() {
        return nodes.get(0);
    }

}
