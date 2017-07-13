package ru.kontur.cajrr.resources;


import com.google.common.collect.Lists;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Range;
import ru.kontur.cajrr.api.Token;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Path("/ring")
@Produces(MediaType.APPLICATION_JSON)
public class RingResource {

    private AtomicLong counter = new AtomicLong();

    private BigInteger RANGE_MIN;

    private BigInteger RANGE_MAX;
    private BigInteger RANGE_SIZE;
    private final AppConfiguration config;

    public RingResource(AppConfiguration config) {
        this.config = config;
    }

    @GET
    @Path("/{keyspace}/{slices}")
    public List<Token> describe(
            @PathParam("keyspace") String keyspace,
            @PathParam("slices") int slices
    ) {
        String partitioner = config.defaultNode().getPartitioner();

        if(partitioner.endsWith("RandomPartitioner")) {
            RANGE_MIN = BigInteger.ZERO;
            RANGE_MAX = new BigInteger("2").pow(127).subtract(BigInteger.ONE);
        } else if (partitioner.endsWith("Murmur3Partitioner")) {
            RANGE_MIN = new BigInteger("2").pow(63).negate();
            RANGE_MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
        }
        RANGE_SIZE = RANGE_MAX.subtract(RANGE_MIN).add(BigInteger.ONE);

        List<String> ranges = config.defaultNode().describeRing(keyspace);

        return getTokensFromRanges(ranges, slices);
    }

    private List<Token> getTokensFromRanges(List<String> lines, int slices) {
        List<Range> ranges = new ArrayList<>();
        for(String line: lines) {
            Range range = new Range(line);
            ranges.add(range);
        }
        ranges.sort(Range::compareTo);

        counter.set(0);
        Token first = null;
        Token prev = null;
        List<Token> result = Lists.newArrayList();

        for (Range range: ranges) {
            try {
                String endpoints = range.endpoints;
                Token token = new Token(range.start, RANGE_MIN, RANGE_MAX, RANGE_SIZE);
                token.endpoint = endpoints;
                if (prev == null) {
                    first = token;
                    prev = token;
                    continue;
                }
                prev.setNext(token);
                prev.fragment(slices, counter);
                result.add(prev);
                prev = token;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        assert prev != null;
        prev.setNext(first);
        prev.fragment(slices, counter);
        result.add(prev);
        return result;
    }

}
