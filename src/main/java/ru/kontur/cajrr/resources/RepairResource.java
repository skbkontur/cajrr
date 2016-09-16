package ru.kontur.cajrr.resources;

import com.codahale.metrics.annotation.Timed;
import ru.kontur.cajrr.api.Repair;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Path("/repair")
@Produces(MediaType.APPLICATION_JSON)
public class RepairResource {


    private final String template;
    private final String defaultName;
    private final AtomicLong counter;


    public RepairResource(String template, String defaultName) {
        this.template = template;
        this.defaultName = defaultName;
        this.counter = new AtomicLong();
    }

    @GET
    @Timed
    public Repair sayHello(@QueryParam("name") Optional<String> name) {
        final String value = String.format(template, name.orElse(defaultName));
        return new Repair(counter.incrementAndGet(), value);
    }
}
