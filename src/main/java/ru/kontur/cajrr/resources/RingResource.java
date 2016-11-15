package ru.kontur.cajrr.resources;


import io.dropwizard.jersey.params.IntParam;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Cluster;
import ru.kontur.cajrr.api.Ring;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@Path("/ring/{index}")
@Produces(MediaType.APPLICATION_JSON)
public class RingResource {

    private final AppConfiguration config;

    public RingResource(AppConfiguration config) {
        this.config = config;
    }

    @GET
    public Ring ring(@PathParam("index") IntParam index) {
        Cluster cluster = retrieveCluster(index);
        return cluster.getRing();
    }

    @GET
    @Path("/describe/{keyspace}")
    public Ring describe(
            @PathParam("index") IntParam index,
            @PathParam("keyspace") NonEmptyStringParam keyspace
    ) throws Exception {
        Cluster cluster = retrieveCluster(index);
        String ks = retrieveKeyspace(keyspace);

        return cluster.describeRing(ks);
    }

    private String retrieveKeyspace(NonEmptyStringParam keyspace) {
        Optional<String> ks = keyspace.get();
        if(!ks.isPresent()) {
            throw new NotFoundException();
        }
        return ks.get();
    }

    private Cluster retrieveCluster(IntParam ind) {
        int index = ind.get();
        if(index<0 || index >= config.clusters.size()) {
            throw new NotFoundException();
        }
        return config.clusters.get(index);
    }
}
