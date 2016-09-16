package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.Length;

/**
 * Created by Kirill Melnikov on 16.09.16.
 *
 */
public class Repair {

    private long id;

    @Length(max = 3)
    private String content;

    public Repair() {
        // Jackson deserialization
    }

    public Repair(long id, String content) {
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
