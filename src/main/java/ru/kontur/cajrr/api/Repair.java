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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Kirill Melnikov on 16.09.16.
 *
 */
public class Repair {


    @JsonProperty
    public long id;

    @JsonProperty
    public String keyspace;

    @JsonProperty
    public String cause;

    @JsonProperty
    public String owner;

    @JsonProperty
    public Map<String, String> options;

    @JsonProperty
    public String callback;

    @JsonProperty
    public String message;

    @JsonProperty
    public Boolean error;

    private RepairStatus status = new RepairStatus();

    public Repair() {
        options = new HashMap<>(5);
        error = false;
    }

    public Repair(long id, Map<String, String> options, String owner,  String cause, String callback) {
        this.id = id;
        this.options = options;
        this.callback = callback;
        this.cause = cause;
        this.owner = owner;

        this.status.id = id;
    }

    public void progress(ProgressEvent event) throws Exception {
        HttpClient httpclient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(callback);

        status.populate(event);
        status.error = error;

        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(status);
        httppost.setEntity(new StringEntity(jsonString, "UTF8"));
        httppost.setHeader("Content-type", "application/json");

//Execute and get the response.
        HttpResponse response = httpclient.execute(httppost);
        HttpEntity entity = response.getEntity();

        if (entity != null) {
            try (InputStream instream = entity.getContent()) {
                instream.close(); // TODO log
            }
        }
    }

    public void error(String message) {
        this.error = true;
        this.message = message;
    }
}

