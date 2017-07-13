package ru.kontur.cajrr.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.lifecycle.Managed;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kontur.cajrr.api.Table;
import ru.kontur.cajrr.resources.RepairResource;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.*;

import static javax.management.JMX.newMBeanProxy;

public class CassandraNode implements Managed {

    private static final Logger LOG = LoggerFactory.getLogger(RepairResource.class);
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
    private static final String aesObjName = "org.apache.cassandra.internal:type=AntiEntropySessions";
    private String host = "localhost";
    private Integer port = 7199;
    private String username;
    private String password;
    private boolean connected;
    private JMXConnector jmxc;
    private StorageServiceMBean ssProxy;
    private MBeanServerConnection mbeanServerConn;
    private String partitioner = null;

    @JsonProperty
    public String getHost() {
        InetAddress address;
        String result;
        try {
            address = InetAddress.getByName(host);
            result = address.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            result = host;
        }
        return result;
    }

    @JsonProperty
    public void setHost(String host) {
        this.host = host;
    }

    @JsonProperty
    public Integer getPort() {
        return port;
    }

    @JsonProperty
    public void setPort(Integer port) {
        this.port = port;
    }

    @JsonProperty
    public String getUsername() {return username;}

    @JsonProperty
    public void setUsername(String username) {this.username = username;}

    @JsonProperty
    public String getPassword() {return password;}

    @JsonProperty
    public void setPassword(String password) {this.password = password;}

    @Override
    public void start() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
        Map<String,Object> env = new HashMap<>();
        if (username != null)
        {
            String[] creds = { username, password };
            env.put(JMXConnector.CREDENTIALS, creds);
        }

        env.put("com.sun.jndi.rmi.factory.socket", getRMIClientSocketFactory());

        jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        mbeanServerConn = jmxc.getMBeanServerConnection();


        try
        {
            ObjectName name = new ObjectName(ssObjName);
            ssProxy = newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
            connected = true;
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(
                    "Invalid ObjectName? Please report this as a bug.", e);
        }
    }

    @Override
    public void stop() throws IOException
    {
        connected = false;
        jmxc.close();
    }

    private RMIClientSocketFactory getRMIClientSocketFactory() throws IOException
    {
        if (Boolean.parseBoolean(System.getProperty("ssl.enable")))
            return new SslRMIClientSocketFactory();
        else
            return RMISocketFactory.getDefaultSocketFactory();
    }

    public String getPartitioner() {
        if(partitioner==null) {
            partitioner = ssProxy.getPartitionerName();
        }
        return partitioner;
    }

    public String getConnectionId() throws IOException {
        return jmxc.getConnectionId();
    }

    /**
     * @return true if any repairs are running on the node.
     */
    public boolean isRepairRunning() {
        // Check if AntiEntropySession is actually running on the node
        try {
            ObjectName name = new ObjectName(aesObjName);
            int activeCount = (Integer) mbeanServerConn.getAttribute(name, "ActiveCount");
            long pendingCount = (Long) mbeanServerConn.getAttribute(name, "PendingTasks");
            return activeCount + pendingCount != 0;
        } catch (IOException ignored) {
            LOG.warn("Failed to connect to " + host + " using JMX");
        } catch (MalformedObjectNameException ignored) {
            LOG.error("Internal error, malformed name");
        } catch (InstanceNotFoundException e) {
            // This happens if no repair has yet been run on the node
            // The AntiEntropySessions object is created on the first repair
            return false;
        } catch (Exception e) {
            LOG.error("Error getting attribute from JMX", e);
        }
        // If uncertain, assume it's running
        return true;
    }

    public int repairAsync(String keyspace, Map<String, String> options) {
        int command = -1;
        try {
            if (!isConnected()) {
                start();
            }
            command = ssProxy.repairAsync(keyspace, options);
        } catch (Exception e) {
            LOG.error(e.getCause().getMessage());
            e.getCause().printStackTrace();
        }
        return command;
    }

    public void addListener(NotificationListener observer) throws IOException {
        if(null==jmxc) {
            start();
        }
        jmxc.addConnectionNotificationListener(observer, null, null);
        ssProxy.addNotificationListener(observer, null, null);
    }

    public void removeListener(NotificationListener observer) {
        try {
            jmxc.removeConnectionNotificationListener(observer, null, null);
            ssProxy.removeNotificationListener(observer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public List<String> describeRing(String keyspace) {
        try {
            return ssProxy.describeRingJMX(keyspace);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Table> getTables(String keyspace) {
        List<Table> result = new ArrayList<>();
        try {
            start();
            ObjectName oName = new ObjectName( String.format("org.apache.cassandra.db:type=ColumnFamilies,keyspace=%s,columnfamily=*", keyspace));

            Set<ObjectName> names = mbeanServerConn.queryNames( oName, null);
            for (ObjectName name: names) {
                String sName = name.getKeyProperty("columnfamily");

                oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=ColumnFamily,keyspace=%s,scope=%s,name=%s", keyspace, sName, "TotalDiskSpaceUsed"));
                long size = newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxCounterMBean.class).getCount();

                result.add(new Table(sName, size));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean isConnected() {
        try {
            String connectionId = getConnectionId();
            return null != connectionId && connectionId.length() > 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
