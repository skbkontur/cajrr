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
    public Repair repairFragment(Repair repair) throws Exception {

        repair.callback = config.callback;

        Cluster cluster = retrieveCluster(repair.cluster);
        cluster.registerRepair(repair);

        return repair;
    }
    private Cluster retrieveCluster(String index) {
        for (Cluster cluster :
                config.clusters) {
            if(cluster.name.equals(index)) {
                return cluster;
            }
        }
        throw new NotFoundException();
    }
}
