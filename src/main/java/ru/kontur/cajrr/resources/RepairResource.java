package ru.kontur.cajrr.resources;

import com.orbitz.consul.Consul;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import java.io.IOException;
import java.time.Duration;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.*;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Path("/repair")
@Produces(MediaType.APPLICATION_JSON)
public class RepairResource {

    private final Consul consul;
    private AtomicBoolean needToRepair = new AtomicBoolean(true);


    private final AppConfiguration config;
    public TableResource tableResource;
    public RingResource ringResource;
    private boolean error;
    private static ObjectMapper mapper = new ObjectMapper();

    public RepairResource(Consul consul, AppConfiguration config) {
        this.consul = consul;
        this.config = config;
    }

    private static final Logger LOG = LoggerFactory.getLogger(RepairResource.class);

    public void run() throws Exception {

        while (needToRepair.get()) {
            RepairStats stats = new RepairStats();
            stats.Cluster = config.cluster;
            stats.ClusterTotal = totalCluster(config.keyspaces);

            for (String keyspace : config.keyspaces) {
                stats.Keyspace = keyspace;
                stats.KeyspaceTotal = totalKeyspace(keyspace);
                stats.KeyspaceCompleted = 0;

                List<Table> tables = tableResource.getTables(keyspace);
                for (Table table : tables) {
                    stats.Table = table.name;
                    stats.TableTotal = totalTable(keyspace, table);
                    stats.TableCompleted = 0;


                    List<Token> tokens = ringResource.describe(keyspace, table.getSlices());
                    for (Token token : tokens) {
                        List<Fragment> ranges = token.getRanges();

                        for (Fragment frag : ranges) {
                            Repair repair = new Repair();
                            repair.started = Instant.now();

                            repair.id = frag.id;
                            repair.cluster = config.cluster;
                            repair.keyspace = keyspace;
                            repair.table = table.name;
                            repair.endpoint = frag.endpoint;
                            repair.start = frag.getStart();
                            repair.end = frag.getEnd();

                            SimpleCondition condition = new SimpleCondition();
                            Duration elapsed = registerRepair(repair, condition);
                            if (error) {
                                stats = stats.errorRepair(repair, elapsed);
                            } else {
                                stats = stats.completeRepair(repair, elapsed);
                            }
                            LOG.info(stats.toString());

                            saveStats(stats);

                            if(!needToRepair.get()) break;
                        }
                        if(!needToRepair.get()) break;
                    }
                    if(!needToRepair.get()) break;
                }
                if(!needToRepair.get()) break;
            }
            Thread.sleep(config.interval);
        }
    }

    private void saveStats(RepairStats stats) throws IOException {
        String jsonString = mapper.writeValueAsString(stats);
        consul.keyValueClient().putValue("/stats", jsonString);
    }


    private int totalKeyspace(String keyspace) {
        int result = 0;
        List<Table> tables = tableResource.getTables(keyspace);

        for (Table table : tables) {
            result += totalTable(keyspace, table);
        }
        return result;
    }

    private int totalTable(String keyspace, Table table) {
        int result = 0;
        List<Token> tokens = ringResource.describe(keyspace, table.getSlices());

        for (Token token : tokens) {
            List<Fragment> ranges = token.getRanges();
            result += ranges.size();
        }
        return result;
    }

    private int totalCluster(List<String> keyspaces) {
        int result = 0;
        for (String keyspace: keyspaces) {
            result += totalKeyspace(keyspace);
        }
        return result;
    }

    @GET
    @Path("/stop")
    public String stop() {
        needToRepair.set(false);
        return "OK";
    }



    private Duration registerRepair(Repair repair, SimpleCondition condition) throws Exception {
        LOG.info("Starting fragment: " + repair.toString());
        Instant start = repair.started;
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
                this.error = false;
            }

            proxy.removeListener(repair);

        }   catch (Exception e) {
            this.error = true;
            e.printStackTrace();
        }
        Instant end = Instant.now();
        return Duration.between(start, end);
    }
}
