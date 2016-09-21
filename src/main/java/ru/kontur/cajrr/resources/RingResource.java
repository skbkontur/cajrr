package ru.kontur.cajrr.resources;


import io.dropwizard.jersey.params.LongParam;
import ru.kontur.cajrr.AppContext;
import ru.kontur.cajrr.api.RepairRange;
import ru.kontur.cajrr.tools.SegmentGenerator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

@Path("/ring")
@Produces(MediaType.APPLICATION_JSON)
public class RingResource {

    private final AppContext context;

    public RingResource(AppContext context) {
        this.context = context;
    }

    @GET
    public Map<String, String> tokenMap() {
        return context.proxy.getTokenToEndpointMap();
    }

    @GET
    @Path("/{slices}")
    public List<String> splitRing(@PathParam("slices") LongParam slices) throws Exception {
        List<String> result = new ArrayList<>();
        Map<String, String> map = context.proxy.getTokenToEndpointMap();
        List<BigInteger> tokens = map.entrySet().stream().map(entry -> BigInteger.valueOf(Long.decode(entry.getKey()))).collect(Collectors.toList());
        List<RepairRange> tokenSegments = generateSegments(tokens, slices.get());
        result.addAll(tokenSegments.stream().map(RepairRange::toString).collect(Collectors.toList()));
        return result;
    }

    private static List<RepairRange> generateSegments(List<BigInteger> tokens, Long segmentCount) throws Exception {
        Collections.sort(tokens, (o1, o2) -> {
            Long i1 = o1.longValue();
            Long i2 = o2.longValue();
            return (i1 < i2 ? -1 : (Objects.equals(i1, i2) ? 0 : 1));
        });

        SegmentGenerator sg = new SegmentGenerator("Murmur3Partitioner");
        return sg.generateSegments(segmentCount, tokens);
    }
}
