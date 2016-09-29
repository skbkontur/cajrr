package ru.kontur.cajrr.resources;


import io.dropwizard.jersey.params.IntParam;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Cluster;
import ru.kontur.cajrr.api.Ring;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/ring/{index}")
@Produces(MediaType.APPLICATION_JSON)
public class RingResource {

    private final AppConfiguration config;

    public RingResource(AppConfiguration config) {
        this.config = config;
    }

    @GET
    public Ring ring(@PathParam("index") IntParam index) {
        return makeRing(index.get());
    }

    private Ring makeRing(int index) {
        if(index<0 || index >= config.clusters.size()) {
            throw new NotFoundException();
        }
        Cluster cluster = config.clusters.get(index);

        return cluster.ring;
    }
}
