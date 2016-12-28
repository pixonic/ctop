package com.pixonic.ctop;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {
    private static volatile boolean shutdown = false;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java -jar ctop.jar <host:jmx_port> <key_space> [interval_sec(default: 10)]");
            return;
        }

        String hostAndPort = args[0];
        String keySpace = args[1];
        int interval = 10;
        if (args.length > 2) {
            interval = Integer.parseInt(args[2]);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown = true));

        System.out.println("Connecting to " + hostAndPort + "...");

        final JMXServiceURL target = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostAndPort + "/jmxrmi");

        final JMXConnector connector = JMXConnectorFactory.connect(target);

        final MBeanServerConnection remote = connector.getMBeanServerConnection();

        ObjectName objectName = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,keyspace=" + keySpace + ",columnfamily=*");

        List<MonitoringEntry> items = new LinkedList<>();
        for (ObjectName mbean : remote.queryNames(objectName, null)) {
            AttributeList counts = remote.getAttributes(mbean, new String[] {"ReadCount", "WriteCount"});
            long readCount = (Long) (((Attribute) counts.get(0)).getValue());
            long writeCount = (Long) (((Attribute) counts.get(1)).getValue());
            if (readCount > 0 || writeCount > 0) items.add(new MonitoringEntry(mbean, readCount, writeCount));
        }

        System.out.println("Connected. Gathering data...");

        while (!shutdown) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(interval));
            Set<ResultItem> readResult = new TreeSet<>();
            Set<ResultItem> writeResult = new TreeSet<>();
            for (MonitoringEntry item : items) {
                MonitoringEntry resultItem = calculateDifference(remote, item);
                if (resultItem.readCount > 0) readResult.add(new ResultItem(item.cf, resultItem.readCount));
                if (resultItem.writeCount > 0) writeResult.add(new ResultItem(item.cf, resultItem.writeCount));
            }

            //clear console
            System.out.print("\033[H\033[2J");
            System.out.flush();

            System.out.println("Cassandra top v0.1");
            System.out.println();
            System.out.println(new Date() + " / " + interval + "s");
            System.out.println();

            int width = SttySupport.getTerminalWidth();
            int height = SttySupport.getTerminalHeight();

            int posWrite = width / 2;
            String leftStr = "Reads", rightStr = "Writes";

            System.out.println(makeLine(leftStr, rightStr, posWrite));
            System.out.println();
            Iterator<ResultItem> readIt = readResult.iterator();
            Iterator<ResultItem> writeIt = writeResult.iterator();
            Long maxReadCount = null, maxWriteCount = null;
            for(int i = 7; i < height; i++) {
                if (readIt.hasNext()) {
                    ResultItem resultItem = readIt.next();
                    if (maxReadCount == null) maxReadCount = resultItem.count;
                    leftStr = formatCounter(resultItem, maxReadCount);
                } else {
                    leftStr = "";
                }
                if (writeIt.hasNext()) {
                    ResultItem resultItem = writeIt.next();
                    if (maxWriteCount == null) maxWriteCount = resultItem.count;
                    rightStr = formatCounter(resultItem, maxWriteCount);
                } else {
                    rightStr = "";
                }
                if (leftStr.length() == 0 && rightStr.length() == 0) break;
                System.out.println(makeLine(leftStr, rightStr, posWrite));
            }
        }
    }

    private static String formatCounter(ResultItem resultItem, long maxCount) {
        int maxLen = String.valueOf(maxCount).length();
        return StringUtils.leftPad(String.valueOf(resultItem.count), maxLen + 1) + " " + resultItem;
    }

    private static String makeLine(String left, String right, int rightPos) {
        if (left.length() > rightPos) left = left.substring(0, rightPos);
        if (right.length() > rightPos) left = right.substring(0, rightPos);
        return StringUtils.rightPad(left, rightPos) + " " + right;
    }

    private static MonitoringEntry calculateDifference(MBeanServerConnection remote, MonitoringEntry item) throws AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException, IOException {
        AttributeList counts = remote.getAttributes(item.cf, new String[] {"ReadCount", "WriteCount"});
        long readCount = (Long) (((Attribute) counts.get(0)).getValue());
        long writeCount = (Long) (((Attribute) counts.get(1)).getValue());

        long readDf = readCount - item.readCount;
        long writeDf = writeCount - item.writeCount;

        item.readCount = readCount;
        item.writeCount = writeCount;
        return new MonitoringEntry(item.cf, readDf, writeDf);
    }

    private static class MonitoringEntry {
        private final ObjectName cf;
        private long readCount;
        private long writeCount;

        public MonitoringEntry(ObjectName cf, long readCount, long writeCount) {
            this.cf = cf;
            this.readCount = readCount;
            this.writeCount = writeCount;
        }
    }

    private static class ResultItem implements Comparable<ResultItem> {
        private final ObjectName cf;
        private final long count;

        public ResultItem(ObjectName cf, long count) {
            this.cf = cf;
            this.count = count;
        }

        @Override public int compareTo(ResultItem o) {
            long d = count - o.count;
            return -(d < 0 ? -1 : (d > 0 ? 1 : 0));
        }

        @Override public String toString() {
            return cf.getKeyProperty("columnfamily");
        }
    }
}
