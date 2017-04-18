package ru.kontur.cajrr.api;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.lifecycle.Managed;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Elasticsearch  implements Managed {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch.class);

    private static HttpClient httpClient = HttpClients.createDefault();
    private static Runnable task;
    @JsonProperty
    public String url;
    @JsonProperty
    public String index;
    @JsonProperty
    public String key;
    @JsonProperty
    public int interval = 1000*15;
    private HttpPost httppost = new HttpPost();
    private AtomicBoolean needPost = new AtomicBoolean(true);
    private ConsulClient consul;
    private String statKey;

    @Override
    public void start() throws Exception {
        needPost.set(true);
        httppost.setHeader("Connection", "close");
        httppost.setHeader("Content-type", "application/json");
        httppost.setHeader("Authorization", key);

        if (task == null) {
            task = () -> {
                while (needPost.get()) {
                    try {
                        postStats();
                        Thread.sleep(interval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                String threadName = Thread.currentThread().getName();
                LOG.info("Elastic " + threadName);
            };
            new Thread(task).start();
        }
    }

    @Override
    public void stop() throws Exception {
        needPost.set(false);
    }

    private void postStats() {
        try {
            String posturl = String.format("%s/%s-%s", url, index, DateTime.now(DateTimeZone.UTC).toString("yyyy.MM.dd"));
            LOG.info(posturl);
            httppost.setURI(URI.create(posturl));
            Response<GetValue> stats = consul.getKVValue(statKey);
            GetValue json = stats.getValue();
            if (json != null) {
                ObjectMapper mapper = new ObjectMapper();
                HashMap result = mapper.readValue(json.getDecodedValue(), HashMap.class);
                result.put("@timestamp", DateTime.now().toString());
                String jsonString = mapper.writeValueAsString(result);
                StringEntity entity = new StringEntity(jsonString, "UTF8");
                httppost.setEntity(entity);
                HttpResponse response = httpClient.execute(httppost);
                if (response.getStatusLine().getStatusCode() != 200) {
                    LOG.warn(response.getStatusLine().getReasonPhrase());
                }
                httppost.releaseConnection();
                LOG.info(jsonString);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setConsul(String consul) {
        this.consul = new ConsulClient(consul);
    }

    public void setStatKey(String statKey) {
        this.statKey = statKey;
    }
}
