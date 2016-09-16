package ru.kontur.cajrr;

import ru.kontur.cajrr.api.Repair;
import ru.kontur.cajrr.tools.CassandraProxy;
import ru.kontur.cajrr.tools.RepairObserver;

import java.io.IOException;

/**
 * Single class to hold all application global interfacing objects,
 * and app global options.
 */

public class AppContext {

    public AppConfiguration config;
    public CassandraProxy proxy;

    public void registerRepair(Repair repair) throws IOException {
        RepairObserver observer = new RepairObserver(repair, proxy);
        try {
            proxy.addListener(observer);
            observer.run();
        }   catch (Exception e)
        {
            throw new IOException(e) ;
        }
        finally
        {
            proxy.removeListener(observer);
        }
    }
}
