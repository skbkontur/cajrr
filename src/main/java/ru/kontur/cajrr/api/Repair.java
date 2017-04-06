package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Repair extends JMXNotificationProgressListener {


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

    @Override
    public boolean isInterestedIn(String tag) {
        return tag.equals("repair:" + command);
    }

    private void processComplete(String input) {
        this.message = input;

        Pattern p = Pattern.compile("Repair command #([0-9]+) finished");
        Matcher m = p.matcher(input);

        if (m.find()) {
            this.command =  Integer.parseInt(m.group(1));
            this.message = "Repair command finished";
        }
        condition.signalAll();
    }

    private void processStart(String input) {
        this.message = input;

        Pattern p = Pattern.compile("Starting repair command #([0-9]+), repairing keyspace ([A-Za-z0-9]+) with repair options \\((.+)\\)");
        Matcher m = p.matcher(input);

        if (m.find()) {
            this.command =  Integer.parseInt(m.group(1));
            this.options = m.group(3);
            this.message = "Starting repair command";
        }

    }

    private void processSuccess(String input) {
        this.message = input;
    }


    private void processProgress(String input) {
        this.message = input;

        Pattern p = Pattern.compile("Repair session ([a-f0-9-]+) for range \\(([-0-9]+),([-0-9]+)\\] ([a-z]+)");
        Matcher m = p.matcher(input);

        if (m.find()) {
            this.session = m.group(1);
            this.message = "Repair session " + m.group(4);
        }
    }

    public Map<String,String> getOptions() {
        Map<String, String> result = new HashMap<>();
        result.put(RepairOption.PARALLELISM_KEY, String.valueOf(RepairParallelism.SEQUENTIAL));
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

    @Override
    public void progress(String tag, ProgressEvent event)
    {
            ProgressEventType type = event.getType();
            switch (type) {
                case COMPLETE:
                    processComplete(event.getMessage());
                    break;
                case START:
                    processStart(event.getMessage());
                    break;
                case SUCCESS:
                    processSuccess(event.getMessage());
                    break;
                case PROGRESS:
                    processProgress(event.getMessage());
                    break;
            }

    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }
}

