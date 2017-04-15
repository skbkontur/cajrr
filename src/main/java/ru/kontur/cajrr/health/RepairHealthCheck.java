package ru.kontur.cajrr.health;

import com.codahale.metrics.health.HealthCheck;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.agent.model.NewService;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Node;

import java.util.Arrays;

public class RepairHealthCheck extends HealthCheck {

    private final ConsulClient consul;
    private AppConfiguration config;

    public RepairHealthCheck(AppConfiguration config) {
        this.config = config;
        this.consul = new ConsulClient(config.consul);

        String appId = String.format("%s_%d", config.serviceName, config.servicePort);
        NewService newService = new NewService();
        newService.setId(appId);
        newService.setTags(Arrays.asList("Cassandra", "Repair"));
        newService.setName(config.serviceName);
        newService.setPort(config.servicePort);

        String url = String.format("http://%s:%d/healthcheck", config.serviceHost, config.servicePort + 1);
        NewService.Check serviceCheck = new NewService.Check();
        serviceCheck.setHttp(url);
        serviceCheck.setInterval("10s");
        newService.setCheck(serviceCheck);

        consul.agentServiceRegister(newService);
    }

    @Override
    protected Result check() throws Exception {
        for (Node n : config.nodes) {
            if (!n.isConnected()) {
                return Result.unhealthy(String.format("Node %s is not connected", n.getHost()));
            }
        }
        return Result.healthy();
    }
}