package ru.kontur.cajrr.health;

import com.codahale.metrics.health.HealthCheck;
import ru.kontur.cajrr.AppConfiguration;

public class RepairHealthCheck extends HealthCheck {

    private AppConfiguration config;

    public RepairHealthCheck(AppConfiguration config) {
        this.config = config;
    }

    @Override
    protected Result check() throws Exception {
            if(!config.isConnected()) {
                return Result.unhealthy(String.format("Cluster %s is not connected", config.cluster));
            }
        return Result.healthy();
    }
}