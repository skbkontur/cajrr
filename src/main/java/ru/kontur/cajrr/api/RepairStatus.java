package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RepairStatus {


    @JsonProperty
    public Integer command;

    @JsonProperty
    public String message;

    @JsonProperty
    public String options;

    @JsonProperty
    public String session;

    @JsonProperty
    public String type;


    private static HttpClient httpClient = HttpClients.createDefault();

    private static ObjectMapper mapper = new ObjectMapper();

    private Repair repair;

    @JsonProperty
    public Repair getRepair() {
        return repair;
    }

    RepairStatus(Repair repair) {
        this.repair = repair;
    }

    private void setMessage(String message) {
        switch (type) {
            case "START":
                processStart(message);
                break;
            case "SUCCESS":
                processSuccess(message);
                break;
            case "COMPLETE":
                processComplete(message);
                break;
            case "PROGRESS":
                processProgress(message);
                break;
            default:
                this.message = message;
        }
    }

    private void processComplete(String input) {
        this.message = input;

        Pattern p = Pattern.compile("Repair command #([0-9]+) finished");
        Matcher m = p.matcher(input);

        if (m.find()) {
            this.command =  Integer.parseInt(m.group(1));
            this.message = "Repair command finished";
        }
    }

    private void processSuccess(String input) {
        this.message = input;
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

    private void processProgress(String input) {
        this.message = input;

        Pattern p = Pattern.compile("Repair session ([a-f0-9-]+) for range \\(([-0-9]+),([-0-9]+)\\] ([a-z]+)");
        Matcher m = p.matcher(input);

        if (m.find()) {
            this.session = m.group(1);
            this.message = "Repair session " + m.group(4);
        }
    }

    public void report(ProgressEvent event, String callback) throws IOException {

        this.type = event.getType().toString();
        setMessage(event.getMessage());

        if(this.type.equals("COMPLETE")) {
            HttpPost httppost = new HttpPost(callback);

            String jsonString = mapper.writeValueAsString(this);
            StringEntity entity = new StringEntity(jsonString, "UTF8");
            httppost.setEntity(entity);
            httppost.setHeader("Content-type", "application/json");

            httpClient.execute(httppost);
        }
    }
}
