package ru.kontur.cajrr;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.health.RepairHealthCheck;
import ru.kontur.cajrr.resources.RepairResource;
import ru.kontur.cajrr.tools.CassandraProxy;

import java.io.IOException;

/**
 * Cajjr
 *
 */
public class App extends Application<AppConfiguration>
{
    static final Logger LOG = LoggerFactory.getLogger(App.class);

    private AppContext context;

    public App() {
        super();
        LOG.info("Default Cajrr constructor called");
        this.context = new AppContext();
    }

    public static void main(String[] args) throws Exception {
        new App().run(args);
    }

    @Override
    public String getName() {
        return "cajrr";
    }

    @Override
    public void initialize(Bootstrap<AppConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/assets/", "/ui", "index.html"));
    }

    @Override
    public void run(AppConfiguration configuration,
                    Environment environment) throws IOException {

        context.config = configuration;

        try {
            context.proxy = new CassandraProxy(
                    configuration.getHost(),
                    configuration.getPort(),
                    configuration.getUsername(),
                    configuration.getPassword()
            );
        } catch (IOException e) {
        }

        final RepairResource resource = new RepairResource(context);
        final RepairHealthCheck healthCheck = new RepairHealthCheck();

        environment.healthChecks().register("repair", healthCheck);
        environment.jersey().register(resource);
    }
}
