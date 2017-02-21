package ru.kontur.cajrr.resources;


import io.dropwizard.jersey.params.IntParam;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Ring;
import ru.kontur.cajrr.api.Token;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Path("/ring")
@Produces(MediaType.APPLICATION_JSON)
public class RingResource {

    private final AppConfiguration config;

    public RingResource(AppConfiguration config) {
        this.config = config;
    }

    @GET
    public List<Token> ring() {
        String partitioner = config.defaultNode().getPartitioner();

        Ring ring = new Ring(partitioner, 1);
        Map<BigInteger, String> map = this.getTokenMap();

        return ring.getTokensFromMap(map);
    }

    private Map<BigInteger,String> getTokenMap() {
        Map<String, String> map = config.defaultNode().getTokenToEndpointMap();
        Map<BigInteger, String> result = new TreeMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            BigInteger key = new BigInteger(entry.getKey());
            String value = entry.getValue();
            result.put(key, value);
        }
        return result;
    }


    @GET
    @Path("/{keyspace}/{slices}")
    public List<Token> describe(
            @PathParam("keyspace") NonEmptyStringParam keyspace,
            @PathParam("slices") IntParam slices
    ) throws Exception {
        String ks = retrieveKeyspace(keyspace);
        String partitioner = config.defaultNode().getPartitioner();
        Ring ring = new Ring(partitioner, slices.get());
        List<String> ranges = config.defaultNode().describeRing(ks);

        return ring.getTokensFromRanges(ranges);
    }

    private String retrieveKeyspace(NonEmptyStringParam keyspace) {
        Optional<String> ks = keyspace.get();
        if(!ks.isPresent()) {
            throw new NotFoundException();
        }
        return ks.get();
    }

}
