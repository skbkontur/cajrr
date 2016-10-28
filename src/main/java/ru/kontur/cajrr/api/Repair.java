package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.HashMap;
import java.util.Map;

public class Repair {


    @JsonProperty
    public long id;

    @JsonProperty
    public String keyspace;

    @JsonProperty
    public Fragment fragment;

    @JsonProperty
    public String callback;

    @JsonProperty
    public String T1;
    
    @JsonProperty
    public String T2;

    private RepairStatus status;



    private static final Logger LOG = LoggerFactory.getLogger(Repair.class);

    public Repair() {
        status = new RepairStatus(this);
    }

    public InputStream progress(ProgressEvent event, boolean sendProgress) throws Exception {
        status.populate(event);

        //Execute and get the response.
        InputStream result = null;
        if(sendProgress) {
            result = postObject(callback, status);
        }
        return result;
    }

    private InputStream postObject(String callback, Object obj)  throws Exception {
        HttpClient httpclient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(callback);

        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(obj);
        httppost.setEntity(new StringEntity(jsonString, "UTF8"));
        httppost.setHeader("Content-type", "application/json");


        HttpResponse response = httpclient.execute(httppost);
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
        result.put("ranges", fragment.toString());
        return result;
    }
}

