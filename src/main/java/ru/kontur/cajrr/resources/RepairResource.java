package ru.kontur.cajrr.resources;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.*;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/repair")
@Produces(MediaType.APPLICATION_JSON)
public class RepairResource {

    public boolean needToRepair = true;

    private final AppConfiguration config;

    public RepairResource(AppConfiguration config) {
        this.config = config;
    }

    private static final Logger LOG = LoggerFactory.getLogger(RepairResource.class);

    public void run(TableResource tableResource, RingResource ringResource) throws Exception {
        while (needToRepair) {
            for (String keyspace : config.keyspaces) {
                List<Table> tables = tableResource.getTables(keyspace);
                for (Table table : tables) {
                    List<Token> tokens = ringResource.describe(keyspace, table.getSlices());
                    for (Token token : tokens) {
                        List<Fragment> ranges = token.getRanges();
                        for (Fragment frag : ranges) {
                            LOG.info("Starting fragment: " + frag.toString());
                            Repair repair = new Repair();
                            repair.id = frag.id;
                            repair.cluster = config.cluster;
                            repair.keyspace = keyspace;
                            repair.table = table.name;
                            repair.endpoint = frag.endpoint;
                            repair.start = frag.getStart();
                            repair.end = frag.getEnd();
                            SimpleCondition condition = new SimpleCondition();
                            registerRepair(repair, condition);
                            if(!needToRepair) break;
                        }
                        if(!needToRepair) break;
                    }
                    if(!needToRepair) break;
                }
                if(!needToRepair) break;
            }
            Thread.sleep(config.interval);
        }
    }

    @GET
    @Path("/stop")
    public String stop() {
        needToRepair = false;
        return "OK";
    }



    private void registerRepair(Repair repair, SimpleCondition condition) throws Exception {
        try {
            Node proxy = config.findNode(repair.getProxyNode());
            repair.setCondition(condition);
            proxy.addListener(repair);
            repair.command = proxy.repairAsync(repair.keyspace, repair.getOptions());

            if (repair.command <= 0)
            {
                LOG.warn(String.format("There is nothing to repair in keyspace %s", repair.keyspace));
            }
            else
            {
                condition.await();
            }

            proxy.removeListener(repair);

        }   catch (Exception e) {
            e.printStackTrace();
        }
    }
}
