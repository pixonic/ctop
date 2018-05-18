package com.pixonic.ctop;

import com.pixonic.ctop.metrics.Metrics;
import com.pixonic.ctop.metrics.MetricsFactory;
import com.pixonic.ctop.metrics.MetricsMode;
import com.pixonic.ctop.util.Constants;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class Main {

    public static final String propertyFileName = "ctop.properties";

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        //customer file passed so load it
        if (args.length > 0) {
            String argument = args[0].trim();
            if (argument.equals("-h")) {
                System.out.println("Usage: java -jar ctop.jar [<propertyFilePath> when not passed ctop.properties will be loaded from classpath]");
                return;
            } else {
                try (InputStream inputStream = new FileInputStream(argument)) {
                    properties.load(inputStream);
                } catch (NullPointerException npe) {
                    System.out.println("File " + argument + " doesn't exist.");
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }
        } else {//load from classpath
            try (InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(propertyFileName)) {
                properties.load(inputStream);
            } catch (NullPointerException npe) {
                System.out.println("File " + propertyFileName + " doesn't exist.");
                return;
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }

        String hostAndPort = properties.getProperty(Constants.CONFIG_HOST, "127.0.0.1").trim() + ":" + properties.getProperty(Constants.CONFIG_JMX_PORT, "7199").trim();
        String keySpace = properties.getProperty(Constants.CASSDB_KEYSPACE).trim();

        int interval = Integer.parseInt(properties.getProperty(Constants.CONFIG_INTERVAL_SEC, "10"));

        String jmxUsername = properties.getProperty(Constants.CONFIG_JMX_USERNAME).trim();
        String jmxPassword = properties.getProperty(Constants.CONFIG_JMX_PASSWORD).trim();

        boolean hasCreds = false;
        String[] creds = null;
        if (jmxUsername != null && jmxPassword != null) {
            creds = new String[]{jmxUsername, jmxPassword};
            hasCreds = true;
        }

        System.out.println("Connecting to " + hostAndPort + "...");

        System.out.println("======= Properties Loaded =======");
        System.out.println(properties.toString());

        final JMXServiceURL target = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostAndPort + "/jmxrmi");

        HashMap environment = new HashMap();
        environment.put(JMXConnector.CREDENTIALS, creds);

        final JMXConnector connector = JMXConnectorFactory.connect(target, (hasCreds) ? environment : null);
        final MBeanServerConnection remote = connector.getMBeanServerConnection();

        ObjectName storageMBean = new ObjectName("org.apache.cassandra.db:type=StorageService");
        String releaseVersion = (String) remote.getAttribute(storageMBean, "ReleaseVersion");
        int majorVersion = Integer.valueOf(releaseVersion.substring(0, releaseVersion.indexOf('.')));
        System.out.println("Cassandra version is " + releaseVersion);

        MetricsMode metricsMode = MetricsMode.valueOf(properties.getProperty(Constants.CONFIG_MERTICS_MODE, "ALL"));

        Metrics metrics = MetricsFactory.getMetrics(majorVersion, interval, remote, keySpace, metricsMode);
        System.out.println("Connected. Gathering data...");
        metrics.printMetrics();

        Runtime.getRuntime().addShutdownHook(new Thread(metrics::shutdown));
    }

}
