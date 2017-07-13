package ru.kontur.cajrr.api;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RepairStats {
    private static ObjectMapper mapper = new ObjectMapper();

    private final ConsulClient consul;
    private final String statKey;
    private final MetricRegistry metrics;
    public String cluster;
    public String keyspace;
    public String table;
    public String token;
    public String host;

    public long ID;
    public int tableTotal;
    public int tableCompleted;
    public int tableErrors;
    public float tablePercent;
    public int keyspaceTotal;
    public int keyspaceCompleted;
    public int keyspaceErrors;
    public float keyspacePercent;
    public int clusterTotal;
    public int clusterCompleted;
    public int clusterErrors;
    public float clusterPercent;
    public String lastClusterSuccess = "";
    Duration duration = java.time.Duration.ZERO;
    Duration tableDuration = java.time.Duration.ZERO;
    Duration tableAverage = java.time.Duration.ZERO;
    Duration tableEstimate = java.time.Duration.ZERO;
    Duration keyspaceDuration = java.time.Duration.ZERO;
    Duration keyspaceAverage = java.time.Duration.ZERO;
    Duration keyspaceEstimate = java.time.Duration.ZERO;
    Duration clusterDuration = java.time.Duration.ZERO;
    Duration clusterAverage = java.time.Duration.ZERO;
    Duration clusterEstimate = java.time.Duration.ZERO;
    private Map<String, Integer> totals;
    private Meter clusterMeter;
    private Meter keyspaceMeter;
    private Meter tableMeter;
    private Meter tokenMeter;
    private Meter clusterErrorMeter;
    private Meter keyspaceErrorMeter;
    private Meter tableErrorMeter;
    private Meter tokenErrorMeter;

    public RepairStats(AppConfiguration config, MetricRegistry metrics, Map<String, Integer> totals) {
        this.metrics = metrics;
        this.consul = new ConsulClient(config.consul);
        this.statKey = config.statKey;
        host = config.serviceHost;
    }

    private static String formatDuration(Duration duration) {
        return DurationFormatUtils.formatDurationHMS(duration.toMillis());
    }

    public RepairStats errorRepair() {
        clusterErrorMeter.mark();
        keyspaceErrorMeter.mark();
        tableErrorMeter.mark();
        tokenErrorMeter.mark();
        return this;
    }

    public RepairStats completeRepair()  {
        clusterMeter.mark();
        keyspaceMeter.mark();
        tableMeter.mark();
        tokenMeter.mark();

        return this;
    }

    private void calculatePercents() {
        clusterPercent = clusterCompleted * 100 / clusterTotal;
        keyspacePercent = keyspaceCompleted * 100 / keyspaceTotal;
        tablePercent = tableCompleted * 100 / tableTotal;

        if (clusterPercent == 100) {
            lastClusterSuccess = LocalDateTime.now().toString();
        }
    }

    private void calculateEstimates() {
        int clusterLeft = clusterTotal - clusterCompleted;
        int keyspaceLeft = keyspaceTotal - keyspaceCompleted;
        int tableLeft = tableTotal - tableCompleted;

        clusterEstimate = clusterAverage.multipliedBy(clusterLeft);
        keyspaceEstimate = keyspaceAverage.multipliedBy(keyspaceLeft);
        tableEstimate = tableAverage.multipliedBy(tableLeft);
    }

    private void calculateAverages() {
        clusterAverage = clusterDuration.dividedBy(clusterCompleted);
        keyspaceAverage = keyspaceDuration.dividedBy(keyspaceCompleted);
        tableAverage = tableDuration.dividedBy(tableCompleted);
    }

    private void incrementErrors() {
        clusterErrors++;
        keyspaceErrors++;
        tableErrors++;
    }

    private void increaseDurations(Duration elapsed) {
        clusterDuration = clusterDuration.plus(elapsed);
        keyspaceDuration = keyspaceDuration.plus(elapsed);
        tableDuration = tableDuration.plus(elapsed);
    }

    @JsonProperty
    public String getDuration() {
        return formatDuration(this.duration);
    }

    @JsonProperty
    public String getClusterDuration() {
        return formatDuration(this.clusterDuration);
    }

    @JsonProperty
    public String getClusterAverage() {
        return formatDuration(this.clusterAverage);
    }

    @JsonProperty
    public String getClusterEstimate() {
        return formatDuration(this.clusterEstimate);
    }

    @JsonProperty
    public String getKeyspaceDuration() {
        return formatDuration(this.keyspaceDuration);
    }

    @JsonProperty
    public String getKeyspaceAverage() {
        return formatDuration(this.keyspaceAverage);
    }

    @JsonProperty
    public String getKeyspaceEstimate() {
        return formatDuration(this.keyspaceEstimate);
    }

    @JsonProperty
    public String getTableDuration() {
        return formatDuration(this.tableDuration);
    }

    @JsonProperty
    public String getTableAverage() {
        return formatDuration(this.tableAverage);
    }

    @JsonProperty
    public String getTableEstimate() {
        return formatDuration(this.tableEstimate);
    }

    public void loadFromJson(String s) {
        try {
            HashMap result = new ObjectMapper().readValue(s, HashMap.class);
            this.cluster = (String) result.get("cluster");
            this.clusterTotal = (int) result.get("clusterTotal");
            this.clusterCompleted = (int) result.get("clusterCompleted");
            this.keyspace = (String) result.get("keyspace");
            this.keyspaceTotal = (int) result.get("keyspaceTotal");
            this.keyspaceCompleted = (int) result.get("keyspaceCompleted");
            this.table = (String) result.get("table");
            this.tableTotal = (int) result.get("tableTotal");
            this.tableCompleted = (int) result.get("tableCompleted");

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

    public void startCluster(String cluster) {
        this.cluster = cluster;
        String name = MetricRegistry.name("cluster", cluster, "repairs");
        clusterMeter = metrics.meter(name);
    }

    public void startKeyspace(String keyspace) {
        this.keyspace = keyspace;
        String name = MetricRegistry.name("keyspace", keyspace, "repairs");
        keyspaceMeter = metrics.meter(name);
    }

    public void initTotalsFromMap(Map<String, Integer> totals) {
        this.totals = totals;
    }

    public void startTable(String table) {
        this.table = table;
        String name = MetricRegistry.name("table", table, "repairs");
        tableMeter = metrics.meter(name);
    }

    public void startToken(String token) {
        this.token = token;
        String name = MetricRegistry.name("token", token, "repairs");
        tokenMeter = metrics.meter(name);

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
}
