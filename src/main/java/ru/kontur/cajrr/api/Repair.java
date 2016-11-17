package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.tools.RepairObserver;

import java.io.InputStream;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class Repair {


    @JsonProperty
    public long id;

    @JsonProperty
    public int cluster;

    @JsonProperty
    public String keyspace;

    @JsonProperty
    public String tables;

    @JsonProperty
    public String start;

    @JsonProperty
    public String end;

    @JsonProperty
    public String callback;

    @JsonProperty
    public String endpoint;

    @JsonProperty
    public String command;

    @JsonProperty
    public String started;


    private RepairStatus status;

    private static final HttpClient httpClient = HttpClients.createDefault();

    private static final Logger LOG = LoggerFactory.getLogger(Repair.class);

    public Repair() {

        status = new RepairStatus(this);
    }

    public InputStream progress(ProgressEvent event, boolean sendProgress) throws Exception {
        status.populate(event);
        status.started = started;

        //Execute and get the response.
        InputStream result = null;
        if(sendProgress) {
            result = postObject(callback, status);
        }
        return result;
    }

    private InputStream postObject(String callback, Object obj)  throws Exception {
        HttpPost httppost = new HttpPost(callback);

        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(obj);
        httppost.setEntity(new StringEntity(jsonString, "UTF8"));
        httppost.setHeader("Content-type", "application/json");


        HttpResponse response = httpClient.execute(httppost);
        HttpEntity entity = response.getEntity();

        if (entity != null) {
            try (InputStream instream = entity.getContent()) {
                instream.close();
                return instream;
            }
        }
        return null;
    }


    public Map<String,String> getOptions() {
        Map<String, String> result = new HashMap<>();
        result.put(RepairOption.PARALLELISM_KEY, String.valueOf(RepairParallelism.PARALLEL));
        result.put(RepairOption.HOSTS_KEY, endpoint);
        if(!tables.equals("") && !tables.equals("*")) {
            result.put(RepairOption.COLUMNFAMILIES_KEY, tables);
        }
        result.put(RepairOption.RANGES_KEY, String.format("%s:%s", start, end));
        return result;
    }

    String GetProxyNode() {
        String[] parts = endpoint.split(",");
        return parts[0].trim();
    }
}

