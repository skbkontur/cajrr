package ru.kontur.cajrr.resources;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.jersey.params.IntParam;
import ru.kontur.cajrr.AppConfiguration;
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
    @Path("/repair/{ring}")
    public Repair repairFragment(
            @PathParam("index") IntParam index,
            Repair repair) throws IOException {
        // Add repair options
        repair.id = counter.incrementAndGet();
        //Node node = config.clusters.get(repair.host);

        //public void registerRepair(Repair repair) throws IOException {
            //RepairObserver observer = new RepairObserver(repair, proxy);
          //  try {
                //proxy.addListener(observer);
                //observer.run();
            //}   catch (Exception e)
            //{
              //  throw new IOException(e) ;
            //}
        //}

        //config.registerRepair(repair);
        return repair;
    }
}
