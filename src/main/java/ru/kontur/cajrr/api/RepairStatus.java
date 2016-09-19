package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RepairStatus {

    @JsonProperty
    public long id;

    @JsonProperty
    public String message;

    @JsonProperty
    public boolean error;


    public RepairStatus() {
        // Jackson deserialization
    }

    public RepairStatus(long id, String message) {
        this.id = id;
        this.message = message;
    }

}
