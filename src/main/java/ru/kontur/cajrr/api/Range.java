package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Range {

    public Long start;

    private Long end;

    @JsonProperty
    public String getStart() {
        return start.toString();
    }

    @JsonProperty
    public String getEnd() {
        return end.toString();
    }

    @JsonProperty
    public String endpoints;


    public Range(String line) {
        parseRange(line);
        parseEndpoints(line);
    }

    private void parseEndpoints(String line) {
        Pattern p = Pattern.compile("endpoints:\\[([^\\]]+)\\]");
        Matcher m = p.matcher(line);

        if (m.find()) {
            this.endpoints =  m.group(1);
        }
    }

    private void parseRange(String line) {
        Pattern p = Pattern.compile("start_token:([-0-9]+), end_token:([-0-9]+)");
        Matcher m = p.matcher(line);

        if (m.find()) {
            this.start =  Long.parseLong(m.group(1));
            this.end = Long.parseLong(m.group(2));
        }
    }

    public int compareTo(Range a) {
        return this.start.compareTo(a.start);
    }

}
