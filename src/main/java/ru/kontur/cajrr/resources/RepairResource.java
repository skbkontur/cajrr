package ru.kontur.cajrr.resources;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.jersey.params.IntParam;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Cluster;
import ru.kontur.cajrr.api.Repair;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

@Path("/repair")
@Produces(MediaType.APPLICATION_JSON)
public class RepairResource {


    private final AppConfiguration config;

    public RepairResource(AppConfiguration config) {
        this.config = config;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @Path("/{index}")
    public Repair repairFragment(
            @PathParam("index") IntParam index,
            Repair repair) throws Exception {

        if(index.get()<0 || index.get() >= config.clusters.size()) {
            throw new NotFoundException();
        }
        Cluster cluster = config.clusters.get(index.get());
        cluster.registerRepair(repair);

        return repair;
    }
}
