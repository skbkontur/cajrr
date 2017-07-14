package ru.kontur.cajrr.resources;

import io.dropwizard.setup.Environment;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.*;
import ru.kontur.cajrr.core.CassandraNode;
import ru.kontur.cajrr.api.Token;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
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
    private final Environment env;
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

    public RepairResource(AppConfiguration config, Environment environment) {

        this.config = config;
        this.env = environment;
    }

    public void run() {
        Map<String, Integer> totals = calculateTotals(config.cluster, config.keyspaces);

        stats = new RepairStats(config, totals);
        int startAt = stats.startPosition();

        while (needToRepair.get()) {
            stats.initTotalsFromMap(totals);
            stats.startCluster(config.cluster, 0);
            int count = 0;

            for (String keyspace : config.keyspaces) {
                stats.startKeyspace(keyspace,0);

                List<Table> tables = tableResource.tables(keyspace);
                for (Table table : tables) {
                    stats.startTable(table.name,0);

                    List<Token> tokens = ringResource.describe(keyspace, table.slices);
                    for (Token token : tokens) {
                        stats.startToken(token.toString(),0);

                        List<Fragment> fragments = token.fragments;
                        for (Fragment frag : fragments) {
                            if (count >= startAt) {

                                runRepair(keyspace, table.name, frag);
                                if (error) {
                                    stats = stats.errorRepair();
                                } else {
                                    stats = stats.completeRepair();
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
                            Arrays.asList(cluster, keyspace, table.name, token.toString()));
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

    private Duration runRepair(String keyspace, String table, Fragment frag) {
        Instant start = Instant.now();

        String endpoint = frag.endpoint;
        String begin = frag.getStart();
        String end = frag.getEnd();
        this.error = false;
        condition = new SimpleCondition();
        LOG.info("Starting fragment: " + begin + ":" + end);
        try {
            CassandraNode proxy = config.findNode(getProxyNode(endpoint));
            command = proxy.repairAsync(keyspace, getOptions(endpoint, table, begin, end));

            if (command <= 0) {
                LOG.warn(String.format("There is nothing to repair in keyspace %s", keyspace));
            } else {
                condition.await();
            }
        } catch (Exception e) {
            this.error = true;
            e.printStackTrace();
        }
        Instant finish = Instant.now();
        return Duration.between(start, finish);
    }

    private Map<String,String> getOptions(String endpoint, String table, String begin, String end) {
        Map<String, String> result = new HashMap<>();
        result.put(RepairOption.PARALLELISM_KEY, String.valueOf(RepairParallelism.PARALLEL));
        result.put(RepairOption.HOSTS_KEY, endpoint);
        if(!table.equals("") && !table.equals("*")) {
            result.put(RepairOption.COLUMNFAMILIES_KEY, table);
        }
        result.put(RepairOption.RANGES_KEY, String.format("%s:%s", begin, end));
        return result;
    }

    private String getProxyNode(String endpoint) {
        String[] parts = endpoint.split(",");
        return parts[0].trim();
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
