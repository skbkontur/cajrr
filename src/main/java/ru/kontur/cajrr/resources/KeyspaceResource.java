package ru.kontur.cajrr.resources;


import io.dropwizard.jersey.params.NonEmptyStringParam;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Cluster;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Optional;

@Path("/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
public class KeyspaceResource {

    private final AppConfiguration config;

    public KeyspaceResource(AppConfiguration config) {
        this.config = config;
    }

    @GET
    @Path("/{index}")
    public List<String> keyspaces(
            @PathParam("index") NonEmptyStringParam index) {
        Cluster cluster = retrieveCluster(index);
        return cluster.getKeyspaces();
    }

    private Cluster retrieveCluster(NonEmptyStringParam ind) {
        Optional<String> index = ind.get();
        if(!index.isPresent()) {
            throw new NotFoundException();
        }
        for (Cluster cluster :
                config.clusters) {
            if(cluster.name.equals(index.get())) {
                return cluster;
            }
        }
        throw new NotFoundException();
    }
}
