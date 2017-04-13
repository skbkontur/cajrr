package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.kv.Value;
import io.dropwizard.lifecycle.Managed;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Elasticsearch  implements Managed {

    private static HttpClient httpClient = HttpClients.createDefault();
    HttpPost httppost;
    private static ObjectMapper mapper = new ObjectMapper();

    @JsonProperty
    public String url;


    @JsonProperty
    public String index;


    @JsonProperty
    public String key;

    @JsonProperty
    public int interval = 1000*15;

    AtomicBoolean needPost = new AtomicBoolean(true);
    private Consul consul;

    @Override
    public void start() throws Exception {
        initPost();
        Runnable task = () -> {
            while (needPost.get()) {
                try {
                    postStats();
                    Thread.sleep(interval);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            String threadName = Thread.currentThread().getName();
            System.out.println("Elastic " + threadName);
        };
        new Thread(task).start();
    }

    @Override
    public void stop() throws Exception {
        needPost.set(false);
    }

    private void initPost() {
        needPost.set(true);
        String posturl = String.format("%s/%s-%s", url, index, DateTime.now().toString("YYYY.MM.DD"));
        httppost = new HttpPost(posturl);
        httppost.setHeader("Connection", "close");
        httppost.setHeader("Content-type", "application/json");
        httppost.setHeader("Authorization", key);
    }

    private void postStats() throws IOException {
        String jsonString = consul.keyValueClient().getValueAsString("/stats").get();
        StringEntity entity = new StringEntity(jsonString, "UTF8");
        httppost.setEntity(entity);
        httpClient.execute(httppost);
        httppost.releaseConnection();
    }

    public void setConsul(Consul consul) {
        this.consul = consul;
    }
}
