package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RepairStats {

    public String cluster;
    public String keyspace;
    public String table;

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

    LocalDateTime lastClusterSuccess;

    private static String formatDuration(Duration duration) {
        return DurationFormatUtils.formatDurationHMS(duration.toMillis());
    }

    public RepairStats errorRepair(Repair repair, Duration elapsed) {
        this.ID = repair.id;
        this.duration = elapsed;
        incrementErrors();
        return this;
    }

    public RepairStats completeRepair(Repair repair, Duration elapsed)  {
        this.ID = repair.id;
        if (this.clusterDuration.isZero() && this.clusterCompleted > 0) {
            this.clusterDuration = elapsed.multipliedBy(clusterCompleted);
            this.keyspaceDuration = elapsed.multipliedBy(keyspaceCompleted);
            this.tableDuration = elapsed.multipliedBy(tableCompleted);
        }
        this.duration = elapsed;
        incrementCompleted();
        increaseDurations(elapsed);
        calculateAverages();
        calculateEstimates();
        calculatePercents();
        return this;
    }

    private void calculatePercents() {
        clusterPercent = clusterCompleted * 100 / clusterTotal;
        keyspacePercent = keyspaceCompleted * 100 / keyspaceTotal;
        tablePercent = tableCompleted * 100 / tableTotal;

        if (clusterPercent == 100) {
            lastClusterSuccess = LocalDateTime.now();
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

    private void incrementCompleted() {
        clusterCompleted++;
        keyspaceCompleted++;
        tableCompleted++;
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


    @JsonGetter(value = "timestamp")
    public String getTimestamp() {
        return LocalDateTime.now().toString();
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
    public String getLastClusterSuccess() {
        if (lastClusterSuccess != null) {
            return lastClusterSuccess.toString();
        } else {
            return "";
        }
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

    public int clearIfCompleted() {
        if (clusterTotal == clusterCompleted) {
            clearCluster();
        }
        if (keyspaceTotal == keyspaceCompleted) {
            clearKeyspace();
        }
        if (tableTotal == tableCompleted) {
            clearTable();
        }
        return clusterCompleted;
    }

    private void clearCluster() {
        clusterCompleted = 0;
        clusterDuration = java.time.Duration.ZERO;
        clearKeyspace();
    }

    private void clearKeyspace() {
        keyspaceCompleted = 0;
        keyspaceDuration = java.time.Duration.ZERO;
        clearTable();
    }

    private void clearTable() {
        tableCompleted = 0;
        tableDuration = java.time.Duration.ZERO;
    }


}
