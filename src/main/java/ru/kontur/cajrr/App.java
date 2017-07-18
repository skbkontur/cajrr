package ru.kontur.cajrr;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.Application;
import io.dropwizard.metrics.MetricsFactory;
import io.dropwizard.metrics.ReporterFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.api.RepairStats;
import ru.kontur.cajrr.core.CassandraNode;
import ru.kontur.cajrr.health.RepairHealthCheck;
import ru.kontur.cajrr.resources.RepairResource;
import ru.kontur.cajrr.resources.RingResource;
import ru.kontur.cajrr.resources.TableResource;

import java.io.IOException;

/**
 * Cajjr
 *
 */
public class App extends Application<AppConfiguration>
{
    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private String name = "cajrr";
    private RingResource ringResource;
    private RepairResource repairResource;
    private TableResource tableResource;

    public App() {
        super();
        LOG.info("Default Cajrr constructor called");
    }

    public static void main(String[] args) throws Exception {
        new App().run(args);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void initialize(Bootstrap<AppConfiguration> bootstrap) {

    }

    @Override
    public void run(AppConfiguration configuration,
                    Environment environment) throws Exception {

        initResources(configuration, environment);
        initHealthcheck(configuration, environment);
        initManagedObjects(configuration, environment);
        repairResource.run();
    }

    private void initManagedObjects(AppConfiguration configuration, Environment environment) {
        if (null != configuration.elastic) {
            configuration.elastic.setStatKey(configuration.statKey);
            configuration.elastic.setConsul(configuration.consul);
            environment.lifecycle().manage(configuration.elastic);
        }

        for(CassandraNode node: configuration.nodes) {
            try {
                node.addListener(repairResource);
                environment.lifecycle().manage(node);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void initHealthcheck(AppConfiguration configuration, Environment environment) {
        final RepairHealthCheck healthCheck = new RepairHealthCheck(configuration);
        environment.healthChecks().register("repair", healthCheck);
    }

    private void initResources(AppConfiguration configuration, Environment environment) {
        repairResource = new RepairResource(configuration);
        ringResource = new RingResource(configuration);
        tableResource = new TableResource(configuration);

        repairResource.ringResource = ringResource;
        repairResource.tableResource = tableResource;
        environment.jersey().register(repairResource);
        environment.jersey().register(tableResource);
        environment.jersey().register(ringResource);

    }
}
