package ru.kontur.cajrr.resources;

import com.google.common.base.Optional;
import ru.kontur.cajrr.AppContext;
import ru.kontur.cajrr.api.Repair;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Path("/repair")
@Produces(MediaType.APPLICATION_JSON)
public class RepairResource {


    private final AppContext context;

    private final AtomicLong counter;


    public RepairResource(AppContext context) {
        this.context = context;
        this.counter = new AtomicLong();
    }

    @POST
    public Repair repairFragment(@Context UriInfo uriInfo,
                                 @QueryParam("host") Optional<String> host,
                                 @QueryParam("port") Optional<String> port,
                                 @QueryParam("keyspace") Optional<String> keyspace,
                                 @QueryParam("owner") Optional<String> owner,
                                 @QueryParam("cause") Optional<String> cause,
                                 @QueryParam("start") Optional<Integer> start,
                                 @QueryParam("finish") Optional<String> finish
                                 ) {

        Map<String, String> options = new HashMap<>();
        // Add repair options
        Repair repair = new Repair(counter.incrementAndGet(), options);
        try {
            context.registerRepair(repair);
        } catch (IOException e) {
            repair.error(e.getMessage());
        }
        return repair;
    }
}
