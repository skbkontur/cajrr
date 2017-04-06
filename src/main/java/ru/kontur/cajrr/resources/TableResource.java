package ru.kontur.cajrr.resources;


import com.orbitz.consul.Consul;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Path("/tables/{keyspace}")
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

    private final AppConfiguration config;
    private final Consul consul;

    public TableResource(Consul consul, AppConfiguration config) {
        this.consul = consul;
        this.config = config;
    }

    @GET
    public List<Table> tables(
            @PathParam("keyspace") NonEmptyStringParam keyspace) {
        String ks = retrieveKeyspace(keyspace);
        return getTables(ks);
    }

    public List<Table> getTables(String keyspace) {
        List<Table> tables = config.defaultNode().getTables(keyspace);
        tables = combineZerosizedTables(tables);
        tables = calculateTableWeights(tables);
        Collections.sort(tables);
        return tables;
    }

    private List<Table> combineZerosizedTables(List<Table> tables) {
        List<Table> result = new ArrayList<>();
        List<String> emptyNames = new ArrayList<>();
        long zeroSize = 0;
        for (Table table: tables) {
            if(table.size > config.combinationThreshold) {
                result.add(table);
            } else {
                emptyNames.add(table.name);
                zeroSize += table.size;
            }
        }
        if(emptyNames.size() > 0) {
            String emptyName = String.join(",", emptyNames);
            Table emptyTable = new Table(emptyName, zeroSize);
            result.add(emptyTable);
        }
        return result;
    }

    private List<Table> calculateTableWeights(List<Table> tables) {
        long max = findMaxSize(tables);

        for (Table table:tables) {
            if (max!=0) {
                double weight = table.size / (double) max;
                table.setWeight(weight, config.minSlicingSize, config.maxSlices);
            }
        }
        return tables;
    }
    private long findMaxSize(List<Table> tables) {
        long max = 0;
        for (Table table:tables) {
            max = table.size>max ? table.size : max;
        }
        return max;
    }

    private String retrieveKeyspace(NonEmptyStringParam keyspace) {
        Optional<String> ks = keyspace.get();
        if(!ks.isPresent()) {
            throw new NotFoundException();
        }
        return ks.get();
    }

}
