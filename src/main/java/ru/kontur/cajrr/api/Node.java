package ru.kontur.cajrr.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.service.StorageServiceMBean;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.IOException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Node {

    private String host = "localhost";
    private Integer port = 7199;
    private String username;
    private String password;

    @JsonProperty
    public String getHost() {
        return host;
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

    Cluster cluster;
    List<Token> tokens;

    /**
     * Create a connection to the JMX agent and setup the M[X]Bean proxies.
     *
     * @throws IOException on connection failures
     */
    public void connect() throws IOException
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
        MBeanServerConnection mbeanServerConn = jmxc.getMBeanServerConnection();

        try
        {
            ObjectName name = new ObjectName(ssObjName);
            ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(
                    "Invalid ObjectName? Please report this as a bug.", e);
        }
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

    public String getPartitioner() { return ssProxy.getPartitionerName(); }

    public void close() throws IOException
    {
        jmxc.close();
    }

    public int repairAsync(String keyspace, Map<String, String> options) {
        return ssProxy.repairAsync(keyspace, options);
    }

    public void addListener(NotificationListener observer) {
        jmxc.addConnectionNotificationListener(observer, null, null);
        ssProxy.addNotificationListener(observer, null, null);
    }
}