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

    private final AtomicLong counter = new AtomicLong();


    public RepairResource(AppConfiguration config) {
        this.config = config;
        counter.set(0);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @Path("/repair/{index}")
    public Repair repairFragment(
            @PathParam("index") IntParam index,
            Repair repair) throws IOException {

        if(index.get()<0 || index.get() >= config.clusters.size()) {
            throw new NotFoundException();
        }
        Cluster cluster = config.clusters.get(index.get());
        repair.id = counter.incrementAndGet();

        cluster.registerRepair(repair);

        return repair;
    }
}
