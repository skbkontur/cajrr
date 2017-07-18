package ru.kontur.cajrr.api;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.codehaus.jackson.map.ObjectMapper;
import ru.kontur.cajrr.AppConfiguration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RepairStats {
    private static ObjectMapper mapper = new ObjectMapper();

    private final ConsulClient consul;
    private final String statKey;
    private String cluster;
    private String keyspace;
    private String table;
    private String token;
    private String host;
    private String lastClusterSuccess = "";

    private Meter clusterRepairs;
    private Meter keyspaceRepairs;
    private Meter tableRepairs;
    private Meter tokenRepairs;
    private int errors;

    private Map<String, Integer> totals;
    public String message;
    public double percentage;
    public int progressCount;

    public RepairStats(AppConfiguration config, Map<String, Integer> totals) {
        this.totals = totals;
        this.consul = new ConsulClient(config.consul);
        this.statKey = config.statKey;
        host = config.serviceHost;
    }

    private static String formatDuration(Duration duration) {
        return DurationFormatUtils.formatDurationHMS(duration.toMillis());
    }

    public RepairStats errorRepair() {
        errors++;
        return this;
    }

    public RepairStats completeRepair()  {
        clusterRepairs.mark();
        keyspaceRepairs.mark();
        tableRepairs.mark();
        tokenRepairs.mark();
        if (clusterRepairs.getPercent()==100) {
            lastClusterSuccess = Instant.now().toString();
        }
        return this;
    }

    @JsonProperty
    public String getElapsed() {
        return formatDuration(clusterRepairs.getElapsed());
    }

    @JsonProperty
    public String getClusterDuration() {
        return formatDuration(clusterRepairs.getDuration());
    }

    @JsonProperty
    public String getClusterAverage() {
        return formatDuration(this.clusterRepairs.getAverage());
    }

    @JsonProperty
    public String getClusterEstimate() {
        return formatDuration(this.clusterRepairs.getEstimate());
    }

    @JsonProperty
    public String getKeyspaceDuration() {
        return formatDuration(this.keyspaceRepairs.getDuration());
    }

    @JsonProperty
    public String getKeyspaceAverage() {
        return formatDuration(this.keyspaceRepairs.getAverage());
    }

    @JsonProperty
    public String getKeyspaceEstimate() {
        return formatDuration(this.keyspaceRepairs.getEstimate());
    }

    @JsonProperty
    public String getTableDuration() {
        return formatDuration(this.tableRepairs.getDuration());
    }

    @JsonProperty
    public String getTableAverage() {
        return formatDuration(this.tableRepairs.getAverage());
    }

    @JsonProperty
    public String getTableEstimate() {
        return formatDuration(this.tableRepairs.getEstimate());
    }

    private void loadFromJson(String s) {
        try {
            HashMap result = new ObjectMapper().readValue(s, HashMap.class);
            this.cluster = (String) result.get("cluster");
            startCluster(cluster, (int) result.get("clusterCompleted"));

            this.keyspace = (String) result.get("keyspace");
            startKeyspace(keyspace, (int) result.get("keyspaceCompleted"));

            this.table = (String) result.get("table");
            startTable(table, (int) result.get("tableCompleted"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private RepairStats readStats() {
        Response<GetValue> keyValueResponse = consul.getKVValue(statKey);
        GetValue json = keyValueResponse.getValue();
        if (json != null) {
            loadFromJson(json.getDecodedValue());
        }

        return this;
    }


    private void saveStats(RepairStats stats) {
        try {
            String jsonString = mapper.writeValueAsString(stats);
            consul.setKVValue(statKey, jsonString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int startPosition() {
        return 0;
    }

    public void startCluster(String cluster, int completed) {
        this.errors = 0;
        this.cluster = cluster;
        clusterRepairs = new Meter(cluster);
        clusterRepairs.setCompleted(completed);
        clusterRepairs.setTotal(totals.get(cluster));

    }

    public void startKeyspace(String keyspace, int completed) {
        this.keyspace = keyspace;
        keyspaceRepairs = new Meter(keyspace);
        String name = String.join("/",
                Arrays.asList(cluster, keyspace));
        keyspaceRepairs.setCompleted(completed);
        keyspaceRepairs.setTotal(totals.get(name));
    }

    public void initTotalsFromMap(Map<String, Integer> totals) {
        this.totals = totals;
    }

    public void startTable(String table, int completed) {
        this.table = table;
        tableRepairs = new Meter(table);
        String name = String.join("/",
                Arrays.asList(cluster, keyspace, table));
        tableRepairs.setTotal(totals.get(name));
        tableRepairs.setCompleted(completed);
    }

    public void startToken(String token, int completed) {
        this.token = token;
        tokenRepairs = new Meter(token);
        String name = String.join("/",
                Arrays.asList(cluster, keyspace, table, token));
        tokenRepairs.setTotal(totals.get(name));
        tokenRepairs.setCompleted(completed);
    }

    @Override
    public String toString() {
        String jsonString = "";
        try {
            jsonString = mapper.writeValueAsString(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    @JsonProperty
    public int getErrors() {
        return errors;
    }

    @JsonProperty
    public String getToken() {
        return token;
    }

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public String getLastClusterSuccess() {
        return lastClusterSuccess;
    }
}
