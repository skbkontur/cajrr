package ru.kontur.cajrr.api;

import io.dropwizard.lifecycle.Managed;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.resources.RepairResource;
import ru.kontur.cajrr.resources.RingResource;
import ru.kontur.cajrr.resources.TableResource;

import java.util.List;


public class Cluster implements Managed {

    private final List<Node> nodes;
    private final RepairResource repairResource;
    private final RingResource ringResource;
    private final TableResource tableResource;

    public Cluster(AppConfiguration configuration,
                   RepairResource repairResource,
                   RingResource ringResource,
                   TableResource tableResource) {
        this.nodes = configuration.nodes;
        this.repairResource = repairResource;
        this.ringResource = ringResource;
        this.tableResource = tableResource;
    }

    @Override
    public void start() throws Exception {
        for (Node node: nodes) {
            node.start();
        }

        repairResource.run(tableResource, ringResource);
    }

    @Override
    public void stop() throws Exception {
        for (Node node: nodes) {
            node.stop();
        }
    }
}
