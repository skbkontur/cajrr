package ru.kontur.cajrr.resources;


import io.dropwizard.jersey.params.LongParam;
import ru.kontur.cajrr.AppContext;
import ru.kontur.cajrr.api.Ring;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Path("/ring")
@Produces(MediaType.APPLICATION_JSON)
public class RingResource {

    private final AppContext context;
    private final AtomicLong counter;

    public RingResource(AppContext context) {

        this.context = context;
        this.counter = new AtomicLong();
    }

    @GET
    public Ring ring() {
        Map<String, String> tokens = context.proxy.getTokenToEndpointMap();
        String name = context.proxy.getClusterName();
        String partitioner = context.proxy.getPartitioner();
        Ring ring = new Ring(name, partitioner, tokens, 1, counter);
        counter.set(0);
        return ring;
    }

    @GET
    @Path("/{slices}")
    public Ring slicedRing(@PathParam("slices") LongParam slices) {
        return makeRing(slices.get());
    }

    private Ring makeRing(long slices) {
        Map<String, String> tokens = context.proxy.getTokenToEndpointMap();
        String name = context.proxy.getClusterName();
        String partitioner = context.proxy.getPartitioner();
        Ring ring = new Ring(name, partitioner, tokens, slices, counter);
        counter.set(0);
        return ring;
    }
}
