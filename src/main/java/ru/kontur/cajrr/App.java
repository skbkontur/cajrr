package ru.kontur.cajrr;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.api.Cluster;
import ru.kontur.cajrr.health.RepairHealthCheck;
import ru.kontur.cajrr.resources.RepairResource;
import ru.kontur.cajrr.resources.RingResource;
import ru.kontur.cajrr.resources.TableResource;

/**
 * Cajjr
 *
 */
public class App extends Application<AppConfiguration>
{
    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private String name = "cajrr";

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
        name = configuration.serviceName;
        String statKey = String.format("stats/%s", configuration.cluster);
        final RingResource ringResource = new RingResource(configuration);
        final RepairResource repairResource = new RepairResource(configuration);
        repairResource.setStatKey(statKey);
        final TableResource tableResource = new TableResource(configuration);
        environment.jersey().register(repairResource);
        environment.jersey().register(tableResource);
        environment.jersey().register(ringResource);

        final RepairHealthCheck healthCheck = new RepairHealthCheck(configuration);
        environment.healthChecks().register("repair", healthCheck);

        Cluster cluster = new Cluster(configuration, repairResource, ringResource, tableResource);
        environment.lifecycle().manage(cluster);


        if (null != configuration.elastic) {
            configuration.elastic.setStatKey(statKey);
            configuration.elastic.setConsul(configuration.consul);
            environment.lifecycle().manage(configuration.elastic);
        }
    }
}
