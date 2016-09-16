package ru.kontur.cajrr;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.health.TemplateHealthCheck;
import ru.kontur.cajrr.resources.RepairResource;

import static com.google.common.base.Objects.firstNonNull;
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

    @VisibleForTesting
    public App(AppContext context) {
        super();
        LOG.info("ReaperApplication constructor called with custom AppContext");
        this.context = context;
    }

    @Override
    public String getName() {
        return "cajrr";
    }

    @Override
    public void initialize(Bootstrap<AppConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/assets/", "/webui", "index.html"));
    }

    @Override
    public void run(AppConfiguration configuration,
                    Environment environment) {
        final RepairResource resource = new RepairResource(
                configuration.getTemplate(),
                configuration.getDefaultName()
        );
        final TemplateHealthCheck healthCheck =
                new TemplateHealthCheck(configuration.getTemplate());
        environment.healthChecks().register("template", healthCheck);

        environment.jersey().register(resource);
        environment.jersey().register(resource);
    }
}
