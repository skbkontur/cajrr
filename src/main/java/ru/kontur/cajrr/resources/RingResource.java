package ru.kontur.cajrr.resources;


import io.dropwizard.jersey.params.IntParam;
import ru.kontur.cajrr.AppContext;
import ru.kontur.cajrr.api.Ring;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.*;

@Path("/ring")
@Produces(MediaType.APPLICATION_JSON)
public class RingResource {

    private final AppContext context;

    public RingResource(AppContext context) {
        this.context = context;
    }

    @GET
    public Ring ring() {
        Map<String, String> tokens = context.proxy.getTokenToEndpointMap();
        return new Ring(tokens, 1);
    }

    @GET
    @Path("/{slices}")
    public Ring slicedRing(@PathParam("slices") IntParam slices) {
        Map<String, String> tokens = context.proxy.getTokenToEndpointMap();
        return new Ring(tokens, slices.get());
    }


}
