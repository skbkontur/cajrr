package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.utils.progress.ProgressEvent;

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


    private Repair repair;

    @JsonProperty
    public Repair getRepair() {
        return repair;
    }

    RepairStatus(Repair repair) {
        this.repair = repair;
    }

    void populate(ProgressEvent event) {
        this.type = event.getType().toString();
        setMessage(event.getMessage());

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
}
