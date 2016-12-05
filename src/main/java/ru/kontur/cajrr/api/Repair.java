package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.remote.JMXConnectionNotification;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;

public class Repair extends JMXNotificationProgressListener {

    private int cmd;


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

    @JsonProperty
    public String callback;

    private RepairStatus status;

    private final Condition condition = new SimpleCondition();


    private static final Logger LOG = LoggerFactory.getLogger(Repair.class);

    public Repair() {
        // JSON deserializer
    }

    @Override
    public boolean isInterestedIn(String tag) {
        return tag.equals("repair:" + cmd);
    }

    void run(Node proxy) throws Exception
    {
        cmd = proxy.repairAsync(keyspace, getOptions());
        if (cmd <= 0)
        {
            LOG.error(String.format("There is nothing to repair in keyspace %s", keyspace));
        }
        else
        {
            condition.await();
        }

        proxy.removeListener(this);
    }

    private void reportStatus(ProgressEvent event)  throws Exception {
        RepairStatus status = new RepairStatus(this);
        status.report(event, callback);
    }


    Map<String,String> getOptions() {
        Map<String, String> result = new HashMap<>();
        result.put(RepairOption.PARALLELISM_KEY, String.valueOf(RepairParallelism.SEQUENTIAL));
        result.put(RepairOption.HOSTS_KEY, endpoint);
        if(!table.equals("") && !table.equals("*")) {
            result.put(RepairOption.COLUMNFAMILIES_KEY, table);
        }
        result.put(RepairOption.RANGES_KEY, String.format("%s:%s", start, end));
        return result;
    }

    String getProxyNode() {
        String[] parts = endpoint.split(",");
        return parts[0].trim();
    }

    @Override
    public void progress(String tag, ProgressEvent event)
    {
        new Thread(tag) {
            public void run() {
                try {
                    reportStatus(event);

                    ProgressEventType type = event.getType();
                    if (type.equals(ProgressEventType.COMPLETE)) {
                        condition.signalAll();
                    }
                } catch (Exception e) {
                    condition.signalAll();
                }
            }
        }.start();
    }
}

