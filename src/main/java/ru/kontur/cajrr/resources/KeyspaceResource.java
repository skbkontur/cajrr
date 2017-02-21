package ru.kontur.cajrr.resources;


import ru.kontur.cajrr.AppConfiguration;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
public class KeyspaceResource {

    private final AppConfiguration config;

    public KeyspaceResource(AppConfiguration config) {
        this.config = config;
    }

    @GET
    public List<String> keyspaces() {
        return config.defaultNode().getKeyspaces();
    }

}
