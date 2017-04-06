package ru.kontur.cajrr.resources;


import com.orbitz.consul.Consul;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Ring;
import ru.kontur.cajrr.api.Token;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Path("/ring")
@Produces(MediaType.APPLICATION_JSON)
public class RingResource {

    private final AppConfiguration config;
    private final Consul consul;

    public RingResource(Consul consul, AppConfiguration config) {
        this.consul = consul;
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
            @PathParam("keyspace") String keyspace,
            @PathParam("slices") int slices
    ) {
        String partitioner = config.defaultNode().getPartitioner();
        Ring ring = new Ring(partitioner, slices);
        List<String> ranges = config.defaultNode().describeRing(keyspace);

        return ring.getTokensFromRanges(ranges);
    }


}
