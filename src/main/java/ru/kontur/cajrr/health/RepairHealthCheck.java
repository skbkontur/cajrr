package ru.kontur.cajrr.health;

import com.codahale.metrics.health.HealthCheck;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Cluster;

public class RepairHealthCheck extends HealthCheck {

    private AppConfiguration config;

    public RepairHealthCheck(AppConfiguration config) {
        this.config = config;
    }

    @Override
    protected Result check() throws Exception {
        for(Cluster cluster: config.clusters) {
            if(!cluster.isConnected()) {
                return Result.unhealthy(String.format("Cluster %s is not connected", cluster.name));
            }
        }
        return Result.healthy();
    }
}