package ru.kontur.cajrr.resources;

import com.codahale.metrics.annotation.Timed;
import ru.kontur.cajrr.AppContext;
import ru.kontur.cajrr.api.Repair;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
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
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public Repair repairFragment(Repair repair) throws IOException {
        // Add repair options
        repair.id = counter.incrementAndGet();
        context.registerRepair(repair);
        return repair;
    }
}
