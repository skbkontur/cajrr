package ru.kontur.cajrr.resources;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.*;
import ru.kontur.cajrr.core.CassandraNode;
import ru.kontur.cajrr.api.Repair;
import ru.kontur.cajrr.api.Token;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Path("/repair")
@Produces(MediaType.APPLICATION_JSON)
public class RepairResource extends JMXNotificationProgressListener {

    private static final Logger LOG = LoggerFactory.getLogger(RepairResource.class);
    private final AppConfiguration config;
    public TableResource tableResource;
    public RingResource ringResource;
    private AtomicBoolean needToRepair = new AtomicBoolean(true);
    private boolean error;

    private int command = 0;
    private SimpleCondition condition;
    private String message;
    private double percentage;
    private int progressCount;
    private RepairStats stats;

    public RepairResource(AppConfiguration config) {
        this.config = config;
    }

    public void run(RepairStats stats) {
        this.stats = stats;
        int startAt = stats.startPosition();

        while (needToRepair.get()) {
            Map<String, Integer> totals = calculateTotals(config.cluster, config.keyspaces);
            stats.initTotalsFromMap(totals);
            stats.startCluster(config.cluster);
            int count = 0;

            for (String keyspace : config.keyspaces) {
                stats.startKeyspace(keyspace);

                List<Table> tables = tableResource.tables(keyspace);
                for (Table table : tables) {
                    stats.startTable(table.name);

                    List<Token> tokens = ringResource.describe(keyspace, table.slices);
                    for (Token token : tokens) {
                        stats.startToken(token.getStart());

                        List<Fragment> fragments = token.fragments;
                        for (Fragment frag : fragments) {
                            if (count >= startAt) {
                                Repair repair = new Repair();
                                repair.started = Instant.now();

                                repair.id = count;
                                repair.cluster = config.cluster;
                                repair.keyspace = keyspace;
                                repair.table = table.name;
                                repair.endpoint = frag.endpoint;
                                repair.start = frag.getStart();
                                repair.end = frag.getEnd();

                                Duration elapsed = runRepair(repair);
                                if (error) {
                                    // TODO - error!
                                    stats = stats.errorRepair(repair, elapsed);
                                } else {
                                    stats = stats.completeRepair(repair, elapsed);
                                }
                                LOG.info(stats.toString());
                            }
                            count++;
                            if (!needToRepair.get()) break;
                        }
                        LOG.info("Token completed");
                    }
                    LOG.info("Table completed: " + table.name);
                }
                LOG.info("Keyspace completed: " + keyspace);
            }
            LOG.info("CassandraCluster completed: " + config.cluster);
            try {
                Thread.sleep(config.interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Map<String, Integer> calculateTotals(String cluster, List<String> keyspaces) {
        HashMap<String, Integer> result = new HashMap<>();
        int clusterTotal = 0;
        for (String keyspace : keyspaces) {
            int keyspaceTotal = 0;
            List<Table> tables = tableResource.tables(keyspace);

            for (Table table : tables) {
                int tableTotal = 0;
                List<Token> tokens = ringResource.describe(keyspace, table.slices);

                for (Token token : tokens) {
                    int tokenTotal = token.fragments.size();
                    String name = String.join("/",
                            Arrays.asList(cluster, keyspace, table.name, token.getStart()));
                    result.put(name, tokenTotal);
                    tableTotal += tokenTotal;
                }
                String name = String.join("/",
                        Arrays.asList(cluster, keyspace, table.name));
                result.put(name, tableTotal);
                keyspaceTotal += tableTotal;
            }
            String name = String.join("/", Arrays.asList(cluster, keyspace));
            result.put(name, keyspaceTotal);
            clusterTotal += keyspaceTotal;
        }
        result.put(cluster, clusterTotal);
        return result;
    }

    private Duration runRepair(Repair repair) {
        this.error = false;
        condition = new SimpleCondition();
        LOG.info("Starting fragment: " + repair.toString());
        Instant start = repair.started;
        try {
            CassandraNode proxy = config.findNode(repair.getProxyNode());
            command = proxy.repairAsync(repair.keyspace, repair.getOptions());

            if (command <= 0) {
                LOG.warn(String.format("There is nothing to repair in keyspace %s", repair.keyspace));
            } else {
                condition.await();
            }
        } catch (Exception e) {
            this.error = true;
            e.printStackTrace();
        }
        Instant end = Instant.now();
        return Duration.between(start, end);
    }


    @GET
    @Path("/stop")
    public String stop() {
        needToRepair.set(false);
        return "OK";
    }

    @GET
    @Path("/stats")
    public String stats() {
        return stats.toString();
    }

    @Override
    public boolean isInterestedIn(String tag) {
        return tag.equals("repair:" + command);
    }

    @Override
    public void progress(String s, ProgressEvent event) {
        ProgressEventType type = event.getType();
        message = event.getMessage();
        percentage = event.getProgressPercentage();
        progressCount = event.getProgressCount();

        switch (type) {
            case COMPLETE:
                condition.signalAll();
                break;
            case START:
                break;
            case SUCCESS:
                break;
            case PROGRESS:
                break;
        }
    }
}
