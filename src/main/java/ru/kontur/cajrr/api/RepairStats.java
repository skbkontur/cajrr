package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.sql.Time;
import java.time.Instant;
import java.time.LocalDateTime;

import java.time.Duration;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RepairStats {

    public String Cluster;
    public String Keyspace;
    public String Table;

    public long ID;
    Duration Duration  = java.time.Duration.ZERO;

    public int TableTotal;
    public int TableCompleted;
    public int TableErrors;
    public float TablePercent;
    Duration TableDuration  = java.time.Duration.ZERO;
    Duration TableAverage  = java.time.Duration.ZERO;
    Duration TableEstimate  = java.time.Duration.ZERO;

    public int KeyspaceTotal;
    public int KeyspaceCompleted;
    public int KeyspaceErrors;
    public float KeyspacePercent;
    Duration KeyspaceDuration  = java.time.Duration.ZERO;
    Duration KeyspaceAverage  = java.time.Duration.ZERO;
    Duration KeyspaceEstimate  = java.time.Duration.ZERO;

    public int ClusterTotal;
    public int ClusterCompleted;
    public int ClusterErrors;
    public float ClusterPercent;

    Duration ClusterDuration  = java.time.Duration.ZERO;
    Duration ClusterAverage  = java.time.Duration.ZERO;
    Duration ClusterEstimate  = java.time.Duration.ZERO;

    LocalDateTime LastClusterSuccess;

    public RepairStats errorRepair(Repair repair, Duration elapsed) {
        this.ID = repair.id;
        this.Duration = elapsed;
        incrementErrors();
        return this;
    }

    public RepairStats completeRepair(Repair repair, Duration elapsed)  {
        this.ID = repair.id;
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

    @JsonSetter(value = "timestamp")
    public void setTimestamp(String ts) {}

    @JsonGetter
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


    private static String formatDuration(Duration duration) {
        return DurationFormatUtils.formatDurationHMS(duration.toMillis());
    }

}
