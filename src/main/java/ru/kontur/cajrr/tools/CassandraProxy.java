package ru.kontur.cajrr.tools;

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
import java.util.Map;

/**
 * Created by Kirill Melnikov on 16.09.16.
 *
 */
public class CassandraProxy {
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
    final String host;
    final Integer port;
    private String username;
    private String password;

    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;
    private StorageServiceMBean ssProxy;

    /**
     * Creates a NodeProbe using the specified JMX host, port, username, and password.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    public CassandraProxy(String host, Integer port, String username, String password) throws IOException
    {
        assert username != null && !username.isEmpty() && password != null && !password.isEmpty()
                : "neither username nor password can be blank";

        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        connect();
    }

    /**
     * Creates a NodeProbe using the specified JMX host and port.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    public CassandraProxy(String host, Integer port) throws IOException
    {
        this.host = host;
        this.port = port;
        connect();
    }

    public void addListener(NotificationListener observer) {
        jmxc.addConnectionNotificationListener(observer, null, null);
        ssProxy.addNotificationListener(observer, null, null);
    }

    public String removeListener(NotificationListener observer) {
        try
        {

            ssProxy.removeNotificationListener(observer);
            jmxc.removeConnectionNotificationListener(observer);
        }
        catch (Throwable e)
        {
            return "Exception occurred during clean-up. " + e;
        }

        return null;
    }

    public int repairAsync(String keyspace, Map<String, String> options) {
        return ssProxy.repairAsync(keyspace, options);
    }
    /**
     * Create a connection to the JMX agent and setup the M[X]Bean proxies.
     *
     * @throws IOException on connection failures
     */
    private void connect() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
        Map<String,Object> env = new HashMap<String,Object>();
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
            ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(
                    "Invalid ObjectName? Please report this as a bug.", e);
        }
    }

    public void close() throws IOException
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
}
