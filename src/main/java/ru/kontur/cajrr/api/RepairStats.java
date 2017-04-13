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

    public String Cluster;
    public String Keyspace;
    public String Table;

    public long ID;
    public int TableTotal;
    public int TableCompleted;
    public int TableErrors;
    public float TablePercent;
    public int KeyspaceTotal;
    public int KeyspaceCompleted;
    public int KeyspaceErrors;
    public float KeyspacePercent;
    public int ClusterTotal;
    public int ClusterCompleted;
    public int ClusterErrors;
    public float ClusterPercent;
    Duration Duration = java.time.Duration.ZERO;
    Duration TableDuration = java.time.Duration.ZERO;
    Duration TableAverage = java.time.Duration.ZERO;
    Duration TableEstimate = java.time.Duration.ZERO;
    Duration KeyspaceDuration = java.time.Duration.ZERO;
    Duration KeyspaceAverage = java.time.Duration.ZERO;
    Duration KeyspaceEstimate = java.time.Duration.ZERO;
    Duration ClusterDuration  = java.time.Duration.ZERO;
    Duration ClusterAverage  = java.time.Duration.ZERO;
    Duration ClusterEstimate  = java.time.Duration.ZERO;

    LocalDateTime LastClusterSuccess;

    private static String formatDuration(Duration duration) {
        return DurationFormatUtils.formatDurationHMS(duration.toMillis());
    }

    public RepairStats errorRepair(Repair repair, Duration elapsed) {
        this.ID = repair.id;
        this.Duration = elapsed;
        incrementErrors();
        return this;
    }

    public RepairStats completeRepair(Repair repair, Duration elapsed)  {
        this.ID = repair.id;
        if (this.ClusterDuration.isZero() && this.ClusterCompleted > 0) {
            this.ClusterDuration = elapsed.multipliedBy(ClusterCompleted);
            this.KeyspaceDuration = elapsed.multipliedBy(KeyspaceCompleted);
            this.TableDuration = elapsed.multipliedBy(TableCompleted);
        }
        this.Duration = elapsed;
        incrementCompleted();
        increaseDurations(elapsed);
        calculateAverage();
        calculateEstimate();
        calculatePercent();
        return this;
    }

    private void calculatePercent() {
        ClusterPercent = ClusterCompleted * 100 / ClusterTotal;
        KeyspacePercent = KeyspaceCompleted * 100 / KeyspaceTotal;
        TablePercent = TableCompleted * 100 / TableTotal;

        if (ClusterPercent == 100) {
            LastClusterSuccess = LocalDateTime.now();
        }
    }

    private void calculateEstimate() {
        int clusterLeft = ClusterTotal - ClusterCompleted;
        int keyspaceLeft = KeyspaceTotal - KeyspaceCompleted;
        int tableLeft = TableTotal - TableCompleted;

        ClusterEstimate = ClusterAverage.multipliedBy(clusterLeft);
        KeyspaceEstimate = KeyspaceAverage.multipliedBy(keyspaceLeft);
        TableEstimate = TableAverage.multipliedBy(tableLeft);
    }

    private void calculateAverage() {
        ClusterAverage = ClusterDuration.dividedBy(ClusterCompleted);
        KeyspaceAverage = KeyspaceDuration.dividedBy(KeyspaceCompleted);
        TableAverage = TableDuration.dividedBy(TableCompleted);

    }

    private void incrementCompleted() {
        ClusterCompleted ++;
        KeyspaceCompleted ++;
        TableCompleted ++;
    }

    private void increaseDurations(Duration elapsed) {
        ClusterDuration = ClusterDuration.plus(elapsed);
        KeyspaceDuration = KeyspaceDuration.plus(elapsed);
        TableDuration = TableDuration.plus(elapsed);
    }

    private void incrementErrors() {
        ClusterErrors ++;
        KeyspaceErrors ++;
        TableErrors ++;

    }

    @JsonGetter(value = "timestamp")
    public String getTimestamp() {
        return LocalDateTime.now().toString();
    }

    @JsonProperty
    public String getDuration() {
        return formatDuration(this.Duration);
    }

    @JsonProperty
    public String getClusterDuration() {
        return formatDuration(this.ClusterDuration);
    }

    @JsonProperty
    public String getClusterAverage() {
        return formatDuration(this.ClusterAverage);
    }

    @JsonProperty
    public String getClusterEstimate() {
        return formatDuration(this.ClusterEstimate);
    }

    @JsonProperty
    public String getKeyspaceDuration() { return formatDuration(this.KeyspaceDuration); }

    @JsonProperty
    public String getKeyspaceAverage() {
        return formatDuration(this.KeyspaceAverage);
    }

    @JsonProperty
    public String getKeyspaceEstimate() {
        return formatDuration(this.KeyspaceEstimate);
    }

    @JsonProperty
    public String getTableDuration() {
        return formatDuration(this.TableDuration);
    }

    @JsonProperty
    public String getTableAverage() {
        return formatDuration(this.TableAverage);
    }

    public void loadFromJson(String s) {
        try {
            HashMap result = new ObjectMapper().readValue(s, HashMap.class);
            this.Cluster = (String) result.get("Cluster");
            this.ClusterTotal = (int) result.get("ClusterTotal");
            this.ClusterCompleted = (int) result.get("ClusterCompleted");
            this.Keyspace = (String) result.get("Keyspace");
            this.KeyspaceTotal = (int) result.get("KeyspaceTotal");
            this.KeyspaceCompleted = (int) result.get("KeyspaceCompleted");
            this.Table = (String) result.get("Table");
            this.TableTotal = (int) result.get("TableTotal");
            this.TableCompleted = (int) result.get("TableCompleted");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int clearIfCompleted() {
        if (ClusterTotal == ClusterCompleted) {
            ClusterCompleted = 0;
            KeyspaceCompleted = 0;
            TableCompleted = 0;
        }
        return ClusterCompleted;
    }
}
