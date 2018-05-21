package com.pixonic.ctop.metrics;

import javax.management.*;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class OldMetrics extends AbstractMetrics {

    private static final String KEY_PROPERTY = "columnfamily";
    private static final String[] ATTRIBUTES = new String[] {"ReadCount", "WriteCount"};

    OldMetrics(long interval, MBeanServerConnection remote, String keySpace, TargetType targetType, MetricsType metricsType, MetricsCollector metricsCollector) {
        super(interval, remote, keySpace, targetType, metricsType, metricsCollector);
    }

    @Override public void printMetrics() throws Exception {
        // when Mode is All then just use *, other types like Keyspace and RegEx can be passed as it is
        String ksValue = targetType.equals(TargetType.ALL) ? "*" : keySpace;
        ObjectName objectName = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,keyspace=" + ksValue + ",columnfamily=*");

        List<MonitoringEntry> items = new LinkedList<>();
        for (ObjectName mbean : remote.queryNames(objectName, null)) {
            AttributeList counts = remote.getAttributes(mbean, ATTRIBUTES);
            long readCount = (Long) (((Attribute) counts.get(0)).getValue());
            long writeCount = (Long) (((Attribute) counts.get(1)).getValue());
            if (readCount > 0 || writeCount > 0) items.add(new MonitoringEntry(mbean, readCount, writeCount));
        }

        while (!shutdown) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(interval));
            NavigableSet<ResultItem> readResult = new TreeSet<>();
            NavigableSet<ResultItem> writeResult = new TreeSet<>();
            for (MonitoringEntry item : items) {
                MonitoringEntry resultItem = calculateDifference(remote, item);
                if (resultItem.readCount > 0)
                    readResult.add(new ResultItem(item.cf, KEY_PROPERTY, resultItem.readCount));
                if (resultItem.writeCount > 0)
                    writeResult.add(new ResultItem(item.cf, KEY_PROPERTY, resultItem.writeCount));
            }

            super.printMetrics(readResult, writeResult);
        }
    }

    private MonitoringEntry calculateDifference(MBeanServerConnection remote, MonitoringEntry item) throws InstanceNotFoundException, IOException, ReflectionException {
        AttributeList counts = remote.getAttributes(item.cf, ATTRIBUTES);
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

        MonitoringEntry(ObjectName cf, long readCount, long writeCount) {
            this.cf = cf;
            this.readCount = readCount;
            this.writeCount = writeCount;
        }
    }

}
