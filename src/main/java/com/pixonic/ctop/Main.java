package com.pixonic.ctop;

import com.pixonic.ctop.metrics.Metrics;
import com.pixonic.ctop.metrics.MetricsFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java -jar ctop.jar <host:jmx_port> <key_space> [interval_sec(default: 10)] [jmxUsername:jmxPassword]");
            return;
        }

        String hostAndPort = args[0];
        String keySpace = args[1];

        int interval = 10;
        if (args.length > 2) {
            interval = Integer.parseInt(args[2]);
        }
        
        boolean hasCreds = false;
        String[] creds = null;
        if (args.length > 3) {
            creds = args[3].trim().split(":", 2);
            hasCreds = true;
        }


        System.out.println("Connecting to " + hostAndPort + "...");


        final JMXServiceURL target = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostAndPort + "/jmxrmi");


        HashMap environment = new HashMap();
        environment.put(JMXConnector.CREDENTIALS, creds);

        final JMXConnector connector = JMXConnectorFactory.connect(target, (hasCreds) ? environment : null);
        final MBeanServerConnection remote = connector.getMBeanServerConnection();

        ObjectName storageMBean = new ObjectName("org.apache.cassandra.db:type=StorageService");
        String releaseVersion = (String) remote.getAttribute(storageMBean, "ReleaseVersion");
        int majorVersion = Integer.valueOf(releaseVersion.substring(0, releaseVersion.indexOf('.')));
        System.out.println("Cassandra version is " + releaseVersion);

        Metrics metrics = MetricsFactory.getMetrics(majorVersion, interval, remote, keySpace);
        System.out.println("Connected. Gathering data...");
        metrics.printMetrics();

        Runtime.getRuntime().addShutdownHook(new Thread(metrics::shutdown));
    }

}
