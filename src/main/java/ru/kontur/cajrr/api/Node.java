package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.lifecycle.Managed;
import org.apache.cassandra.service.StorageServiceMBean;

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
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

public class Node implements Managed {

    private String host = "localhost";
    private Integer port = 7199;
    private String username;
    private String password;

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


    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";

    private JMXConnector jmxc;
    private StorageServiceMBean ssProxy;
    private MBeanServerConnection mbeanServerConn;

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
        jmxc.close();
    }

    private RMIClientSocketFactory getRMIClientSocketFactory() throws IOException
    {
        if (Boolean.parseBoolean(System.getProperty("ssl.enable")))
            return new SslRMIClientSocketFactory();
        else
            return RMISocketFactory.getDefaultSocketFactory();
    }


    public Map<String, String> getTokenToEndpointMap()
    {
        return ssProxy.getTokenToEndpointMap();
    }

    private String partitioner = null;

    public String getPartitioner() {
        if(partitioner==null) {
            partitioner = ssProxy.getPartitionerName();
        }
        return partitioner;
    }


    public int repairAsync(String keyspace, Map<String, String> options) {
        return ssProxy.repairAsync(keyspace, options);
    }

    public void addListener(NotificationListener observer) {
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

}
