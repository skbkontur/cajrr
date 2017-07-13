package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Repair {


    @JsonProperty
    public long id;

    @JsonProperty
    public String start;

    @JsonProperty
    public String end;

    @JsonProperty
    public String endpoint;

    @JsonProperty
    public String cluster;

    @JsonProperty
    public String keyspace;

    @JsonProperty
    public String table;

    private Condition condition;


    private static final Logger LOG = LoggerFactory.getLogger(Repair.class);

    @JsonProperty
    public int command;

    @JsonProperty
    public String message;

    @JsonProperty
    public String options;

    @JsonProperty
    public String session;

    @JsonProperty
    public String type;

    public Instant started;


    public Repair() {
        // JSON deserializer
    }

    public Map<String,String> getOptions() {
        Map<String, String> result = new HashMap<>();
        result.put(RepairOption.PARALLELISM_KEY, String.valueOf(RepairParallelism.PARALLEL));
        result.put(RepairOption.HOSTS_KEY, endpoint);
        if(!table.equals("") && !table.equals("*")) {
            result.put(RepairOption.COLUMNFAMILIES_KEY, table);
        }
        result.put(RepairOption.RANGES_KEY, String.format("%s:%s", start, end));
        return result;
    }

    public String getProxyNode() {
        String[] parts = endpoint.split(",");
        return parts[0].trim();
    }
}

