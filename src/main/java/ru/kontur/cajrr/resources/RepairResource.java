package ru.kontur.cajrr.resources;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.jersey.params.IntParam;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Cluster;
import ru.kontur.cajrr.api.Repair;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Optional;
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
            @PathParam("index") NonEmptyStringParam index,
            Repair repair) throws Exception {

        Cluster cluster = retrieveCluster(index);
        cluster.registerRepair(repair);

        return repair;
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
