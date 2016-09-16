package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.Length;

public class RepairStatus {
    private long id;

    @Length(max = 3)
    private String content;

    public RepairStatus() {
        // Jackson deserialization
    }

    public RepairStatus(long id, String content) {
        this.id = id;
        this.content = content;
    }

    @JsonProperty
    public long getId() {
        return id;
    }

    @JsonProperty
    public String getContent() {
        return content;
    }
}
