package ru.kontur.cajrr;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import ru.kontur.cajrr.core.CassandraNode;
import ru.kontur.cajrr.core.ElasticsearchHandler;

import java.util.ArrayList;
import java.util.List;

public class AppConfiguration extends Configuration {

    @JsonProperty
    public int combinationThreshold = 1000000;

    @JsonProperty
    public int minSlicingSize = 100000000;

    @JsonProperty
    public int maxSlices = 50;

    @JsonProperty
    public int interval = 60*60*24*7;

    @JsonProperty
    public String cluster;

    @JsonProperty
    public List<CassandraNode> nodes = new ArrayList<>();

    @JsonProperty
    public List<String> keyspaces = new ArrayList<>();

    @JsonProperty
    public List<String> exclude = new ArrayList<>();

    @JsonProperty
    public ElasticsearchHandler elastic;

    @JsonProperty
    public String serviceName = "cajrr";

    @JsonProperty
    public String serviceHost = "localhost";

    @JsonProperty
    public Integer servicePort = 8080;

    @JsonProperty
    public String consul = "localhost:8500";

    @JsonProperty
    public String statKey = "repair";


    public CassandraNode findNode(String endpoint) {

        for(CassandraNode node: nodes) {
            String host = node.getHost();
            if (host.equals(endpoint)) {
                return node;
            }
        }
        return defaultNode();
    }
    public CassandraNode defaultNode() {
        return nodes.get(0);
    }

}
