package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.utils.progress.ProgressEvent;

import java.util.Map;

/**
 * Created by Kirill Melnikov on 16.09.16.
 *
 */
public class Repair {

    private long id;

    private String keyspace;
    private String message;
    private boolean error = false;

    private Map<String, String> options;

    public Repair() {
        // Jackson deserialization
    }

    public Repair(long id, Map<String, String> options) {
        this.id = id;
        this.options = options;
    }

    @JsonProperty
    public long getId() {
        return id;
    }

    @JsonProperty
    public Map<String, String> getOptions() {
        return options;
    }

    @JsonProperty
    public String getMessage()  {return message;}



    @JsonProperty
    public String getKeyspace()  {return keyspace;}

    @JsonProperty
    public boolean getError()  {return error;}


    public void setMessage(String message) {
        this.message = message;
   }

    public void error(String message) {
        setMessage(message);
        this.error = true;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void progress(ProgressEvent event) {
        // todo
    }
}

