package ru.kontur.cajrr.health;

import com.codahale.metrics.health.HealthCheck;

public class RepairHealthCheck extends HealthCheck {

    public RepairHealthCheck() {
    }

    @Override
    protected Result check() throws Exception {
        // TODO check connection readyness
        return Result.healthy();
    }
}