package com.pixonic.ctop;

import com.pixonic.ctop.metrics.*;
import com.pixonic.ctop.util.Constants;
import com.pixonic.ctop.util.MetricUtils;

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

    public void init(Properties properties) throws Exception {
        String hostAndPort = properties.getProperty(Constants.CONFIG_HOST, "127.0.0.1").trim() + ":" + properties.getProperty(Constants.CONFIG_JMX_PORT, "7199").trim();
        String keySpace = properties.getProperty(Constants.CASSDB_KEYSPACE).trim();

        int interval = Integer.parseInt(properties.getProperty(Constants.CONFIG_INTERVAL_SEC, "10"));

        String jmxUsername = properties.containsKey(Constants.CONFIG_JMX_USERNAME) ? properties.getProperty(Constants.CONFIG_JMX_USERNAME).trim() : null;
        String jmxPassword = properties.containsKey(Constants.CONFIG_JMX_PASSWORD) ? properties.getProperty(Constants.CONFIG_JMX_PASSWORD).trim() : null;

        boolean hasCreds = false;
        String[] creds = null;
        if (jmxUsername != null && jmxPassword != null) {
            creds = new String[]{jmxUsername, jmxPassword};
            hasCreds = true;
        }

        System.out.println("Connecting to " + hostAndPort + "...");

        System.out.println("======= Properties Loaded =======");
        System.out.println(properties.toString());

        MetricsType metricsType = MetricsType.valueOf(properties.getProperty("metrics.type", "CONSOLE"));

        MetricsCollector metricsCollector = MetricUtils.registerMetricsReporter(metricsType, properties);

        final JMXServiceURL target = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostAndPort + "/jmxrmi");

        HashMap<String, Object> environment = new HashMap<>();
        environment.put(JMXConnector.CREDENTIALS, creds);

        final JMXConnector connector = JMXConnectorFactory.connect(target, (hasCreds) ? environment : null);
        final MBeanServerConnection remote = connector.getMBeanServerConnection();

        ObjectName storageMBean = new ObjectName("org.apache.cassandra.db:type=StorageService");
        String releaseVersion = (String) remote.getAttribute(storageMBean, "ReleaseVersion");
        int majorVersion = Integer.valueOf(releaseVersion.substring(0, releaseVersion.indexOf('.')));
        System.out.println("Cassandra version is " + releaseVersion);

        TargetType targetType = TargetType.valueOf(properties.getProperty(Constants.CONFIG_TARGET_TYPE, "ALL"));

        Metrics metrics = MetricsFactory.getMetrics(majorVersion, interval, remote, keySpace, targetType, metricsType, metricsCollector);
        System.out.println("Connected. Gathering data...");
        metrics.printMetrics();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> metrics.shutdown()));
    }


    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        //customer file passed so load it
        if (args.length > 0) {
            String argument = args[0].trim();
            if (argument.equals("-h")) {
                printHelpUsage();
                return;
            } else if (argument.equals("-p")) {//passing a property file
                //when a file argument is passed then it will be considered
                if (args.length > 1) {
                    String fileName = args[1].trim();
                    try (InputStream inputStream = new FileInputStream(fileName)) {
                        properties.load(inputStream);
                    } catch (NullPointerException npe) {
                        System.out.println("File " + fileName + " doesn't exist.");
                        return;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return;
                    }
                } else {//otherwise load from classpath
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
            } else {//default and legacy approach for ctop with runtime arguments
                //only host is provided and keyspace missing
                if (args.length == 1) {
                    System.out.println("Keyspace is mandatory! Please refer to the usage section.");
                    printHelpUsage();
                    return;
                }
                String hostAndPort = args[0].trim();
                String keySpace = args[1].trim();
                int interval = 10;
                if (args.length > 2) {
                    interval = Integer.parseInt(args[2].trim());
                }

                //load the properties object with passed arguments
                String[] hostInfo = hostAndPort.split(":", 2);
                properties.put(Constants.CONFIG_HOST, hostInfo[0]);
                properties.put(Constants.CONFIG_JMX_PORT, hostInfo[1]);

                properties.put(Constants.CONFIG_INTERVAL_SEC, interval);
                properties.put(Constants.CASSDB_KEYSPACE, keySpace);
            }
        } else {
            printHelpUsage();
            return;
        }

        Main objMain = new Main();
        objMain.init(properties);
    }

    public static void printHelpUsage() {
        String newline = System.getProperty("line.separator");
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("============================" + newline);
        stringBuilder.append("==  Usage  =================" + newline);
        stringBuilder.append("============================" + newline);
        stringBuilder.append("java -jar ctop.jar -p [<propertyFilePath> when not passed ctop.properties will be loaded from classpath with argument -p]" + newline);
        stringBuilder.append("java -jar ctop.jar <host:jmx_port> <key_space> [interval_sec(default: 10)] when doesn't want to use any properties file" + newline);
        stringBuilder.append("java -jar ctop.jar -h to print help" + newline);
        stringBuilder.append("============================");

        System.out.println(stringBuilder.toString());
    }
}
