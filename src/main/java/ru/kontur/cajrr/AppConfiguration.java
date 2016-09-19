package ru.kontur.cajrr;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

/**
 * Created by Kirill Melnikov on 16.09.16.
 *
 */
public class AppConfiguration extends Configuration {
    private String host = "localhost";

    private Integer port = 7199;

    private String username = null;

    private String password = null;

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public void setHost(String host) {
        this.host = host;
    }

    @JsonProperty
    public Integer getPort() {
        return port;
    }

    @JsonProperty
    public void setPort(Integer port) {
        this.port = port;
    }

    @JsonProperty
    public String getUsername() {return username;}

    @JsonProperty
    public void setUsername(String username) {this.username = username;}

    @JsonProperty
    public String getPassword() {return password;}

    @JsonProperty
    public void setPassword(String password) {this.password = password;}
}
