package ru.kontur.cajrr;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import ru.kontur.cajrr.api.Cluster;

import java.util.ArrayList;
import java.util.List;

public class AppConfiguration extends Configuration {
    @JsonProperty
    public String callback = "localhost:8888";

    @JsonProperty
    public List<Cluster> clusters = new ArrayList<>();

}
