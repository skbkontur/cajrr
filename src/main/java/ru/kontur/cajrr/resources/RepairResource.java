package ru.kontur.cajrr.resources;

import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.AppConfiguration;
import ru.kontur.cajrr.api.Node;
import ru.kontur.cajrr.api.Repair;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/repair")
@Produces(MediaType.APPLICATION_JSON)
public class RepairResource {


    private final AppConfiguration config;

    public RepairResource(AppConfiguration config) {
        this.config = config;
    }

    private static final Logger LOG = LoggerFactory.getLogger(RepairResource.class);

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public Repair repairFragment(Repair repair) throws Exception {

        repair.callback = config.callback;

        registerRepair(repair);

        return repair;
    }

    private void registerRepair(Repair repair) throws Exception {
        Node proxy = config.findNode(repair.getProxyNode());
        try {
            proxy.addListener(repair);
            repair.run(proxy);
        }   catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
