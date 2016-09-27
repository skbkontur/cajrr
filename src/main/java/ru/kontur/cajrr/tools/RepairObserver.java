package ru.kontur.cajrr.tools;

import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.api.Repair;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.locks.Condition;

/**
 * Created by Kirill Melnikov on 16.09.16.
 *
 */
public class RepairObserver  extends JMXNotificationProgressListener {

    private Repair repair;
    private final CassandraProxy proxy;
    private final Condition condition = new SimpleCondition();
    private int cmd;

    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    private volatile boolean hasNotificationLost;
    private volatile Exception error;


    private static final Logger LOG = LoggerFactory.getLogger(RepairObserver.class);

    public RepairObserver(Repair repair, CassandraProxy proxy) {

        this.repair = repair;
        this.proxy = proxy;
    }


    public void run() throws Exception
    {
        String keyspace = repair.keyspace;
        cmd = proxy.repairAsync(keyspace, repair.options);
        if (cmd <= 0)
        {
            LOG.error(String.format("There is nothing to repair in keyspace %s", keyspace));
        }
        else
        {
            condition.await();
            if (error != null)
            {
                LOG.error(error.toString());
            }
            if (hasNotificationLost)
            {

                LOG.error(String.format("There were some lost notification(s). You should check server log for repair status of keyspace %s", keyspace));
            }
        }
    }

    @Override
    public boolean isInterestedIn(String tag) {
        return tag.equals("repair:" + cmd);
    }

    @Override
    public void handleNotificationLost(long timestamp, String message)
    {
        hasNotificationLost = true;
    }
    @Override
    public void handleConnectionClosed(long timestamp, String message)
    {
        handleConnectionFailed(timestamp, message);
    }

    @Override
    public void handleConnectionFailed(long timestamp, String message)
    {
        error = new IOException(String.format("[%s] JMX connection closed. You should check server log for repair status of keyspace %s"
                        + "(Subsequent keyspaces are not going to be repaired).",
                format.format(timestamp), repair.keyspace));
        condition.signalAll();
    }

    @Override
    public void progress(String tag, ProgressEvent event)
    {
        try {
            repair.progress(event); // call back progress
            ProgressEventType type = event.getType();
            if (type == ProgressEventType.COMPLETE)
            {
                condition.signalAll();
            }
        } catch (Exception e) {
            error = e;
            condition.signalAll();
        }
    }
}
