package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import javafx.scene.control.Tab;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class Cluster {
    private final int COMBINATION_THRESHOLD = 10000;

    private boolean connected = false;

    @JsonProperty
    public String name;

    @JsonProperty
    public List<Node> nodes;

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
        Node proxy = findNode(repair.getProxyNode());
        try {
            proxy.addListener(repair);
            repair.run(proxy);
        }   catch (Exception e) {
            LOG.error(e.getMessage());
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

    public List<String> getKeyspaces() {
        return defaultNode().getKeyspaces();
    }

    public List<Table> getTables(String keyspace) {
        List<Table> tables = defaultNode().getTables(keyspace);
        tables = combineZerosizedTables(tables);
        tables = calculateTableWeights(tables);
        Collections.sort(tables);
        return tables;
    }

    private List<Table> combineZerosizedTables(List<Table> tables) {
        List<Table> result = new ArrayList<>();
        List<String> emptyNames = new ArrayList<>();
        long zeroSize = 0;
        for (Table table: tables) {
            if(table.size>COMBINATION_THRESHOLD) {
                result.add(table);
            } else {
                emptyNames.add(table.name);
                zeroSize += table.size;
            }
        }
        if(emptyNames.size()>0) {
            String emptyName = String.join(",", emptyNames);
            Table emptyTable = new Table(emptyName, zeroSize);
            result.add(emptyTable);
        }
        return result;
    }

    private List<Table> calculateTableWeights(List<Table> tables) {
        long max = findMaxSize(tables);

        for (Table table:tables) {
            if (max!=0) {
                double weight = table.size / (double) max;
                table.setWeight(weight);
            }
        }
        return tables;
    }

    private long findMaxSize(List<Table> tables) {
        long max = 0;
        for (Table table:tables) {
            max = table.size>max ? table.size : max;
        }
        return max;
    }


}
