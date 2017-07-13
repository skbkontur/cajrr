package ru.kontur.cajrr.resources;

import io.dropwizard.jersey.params.NonEmptyStringParam;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Path("/tables/{keyspace}")
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

    private final AppConfiguration config;

    public TableResource(AppConfiguration config) {
        this.config = config;
    }

    @GET
    public List<Table> tables(
            @PathParam("keyspace") String keyspace) {
        List<Table> tables = config.defaultNode().getTables(keyspace);
        tables = stripExclusions(tables);
        tables = combineSmallTables(tables);
        tables = calculateTableWeights(tables);
        Collections.sort(tables);
        return tables;
    }

    private List<Table> stripExclusions(List<Table> tables) {
        return tables.stream()
                .filter(x -> config.exclude.indexOf(x.name)==-1)
                .collect(Collectors.toList());
    }

    private List<Table> combineSmallTables(List<Table> tables) {
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
}
