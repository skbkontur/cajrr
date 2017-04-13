package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.orbitz.consul.Consul;
import io.dropwizard.lifecycle.Managed;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Elasticsearch  implements Managed {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch.class);

    private static HttpClient httpClient = HttpClients.createDefault();
    @JsonProperty
    public String url;
    @JsonProperty
    public String index;
    @JsonProperty
    public String key;
    @JsonProperty
    public int interval = 1000*15;
    private HttpPost httppost;
    private AtomicBoolean needPost = new AtomicBoolean(true);
    private Consul consul;

    @Override
    public void start() throws Exception {
        initPost();
        Runnable task = () -> {
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

    @Override
    public void stop() throws Exception {
        needPost.set(false);
    }

    private void initPost() {
        needPost.set(true);
        String posturl = String.format("%s/%s-%s", url, index, DateTime.now().toString("YYYY.MM.dd"));
        httppost = new HttpPost(posturl);
        httppost.setHeader("Connection", "close");
        httppost.setHeader("Content-type", "application/json");
        httppost.setHeader("Authorization", key);
    }

    private void postStats() {
        try {
            Optional<String> stats = consul.keyValueClient().getValueAsString("/stats");
            if (stats.isPresent()) {
                String jsonString = stats.get();
                jsonString = jsonString.replace("cluster", "Cluster");
                jsonString = jsonString.replace("keyspace", "Keyspace");
                jsonString = jsonString.replace("table", "Table");
                jsonString = jsonString.replace("duration", "Duration");
                jsonString = jsonString.replace("timestamp", "@timestamp");

                StringEntity entity = new StringEntity(jsonString, "UTF8");
                httppost.setEntity(entity);
                HttpResponse response = httpClient.execute(httppost);
                if (response.getStatusLine().getStatusCode() != 200) {
                    LOG.warn(response.getStatusLine().getReasonPhrase());
                }
                httppost.releaseConnection();
                LOG.info(jsonString);
            } else {
                LOG.warn("Repair stats not found");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setConsul(Consul consul) {
        this.consul = consul;
    }
}
