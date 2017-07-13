package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Range {

    @JsonProperty
    public BigInteger start;

    @JsonProperty
    public BigInteger end;

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
            this.start =  new BigInteger(m.group(1));
            this.end = new BigInteger(m.group(2));
        }
    }

    public int compareTo(Range a) {
        return this.start.compareTo(a.start);
    }

}
