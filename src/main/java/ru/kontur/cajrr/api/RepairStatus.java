package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RepairStatus {

    @JsonProperty
    public long id;

    @JsonProperty
    public String message;

    @JsonProperty
    public boolean error;

    @JsonProperty
    public String type;

    @JsonProperty
    public int count;

    @JsonProperty
    public int total;


    public RepairStatus() {
        // Jackson deserialization
    }

    public RepairStatus(long id, String message) {
        this.id = id;
        this.message = message;
    }

}
