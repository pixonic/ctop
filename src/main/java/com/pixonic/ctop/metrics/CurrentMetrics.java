package com.pixonic.ctop.metrics;

import javax.management.*;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class CurrentMetrics extends AbstractMetrics {

    private static final String KEY_PROPERTY = "scope";
    private static final String ATTRIBUTE = "Count";

    CurrentMetrics(long interval, MBeanServerConnection remote, String keySpace, MetricsMode metricsMode) {
        super(interval, remote, keySpace, metricsMode);
    }

    @Override
    public void printMetrics() throws Exception {
        String ksValue = metricsMode.equals(MetricsMode.ALL) ? "*" : keySpace;
        ObjectName readObjectName = new ObjectName("org.apache.cassandra.metrics:type=Table,keyspace=" + ksValue + ",scope=*,name=ReadLatency");
        ObjectName writeObjectName = new ObjectName("org.apache.cassandra.metrics:type=Table,keyspace=" + ksValue + ",scope=*,name=WriteLatency");

        List<MonitoringEntry> readItems = getMonitoringEntryList(remote, readObjectName);
        List<MonitoringEntry> writeItems = getMonitoringEntryList(remote, writeObjectName);

        while (!shutdown) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(interval));
            super.printMetrics(createResultItems(readItems), createResultItems(writeItems));
        }
    }

    private NavigableSet<ResultItem> createResultItems(List<MonitoringEntry> monitoringItems) throws AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException, IOException {
        NavigableSet<ResultItem> resultItems = new TreeSet<>();

        for (MonitoringEntry item : monitoringItems) {
            MonitoringEntry monitoringEntry = calculateDifference(remote, item);
            if (monitoringEntry.count > 0)
                resultItems.add(new ResultItem(item.objectName, KEY_PROPERTY, monitoringEntry.count));
        }

        return resultItems;
    }

    private MonitoringEntry calculateDifference(MBeanServerConnection remote, MonitoringEntry item) throws AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException, IOException {
        Long count = (Long) remote.getAttribute(item.objectName, ATTRIBUTE);

        long diff = count - item.count;

        item.count = count;
        return new MonitoringEntry(item.objectName, diff);
    }

    private List<MonitoringEntry> getMonitoringEntryList(MBeanServerConnection remote, ObjectName objectName) throws IOException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException {
        List<MonitoringEntry> monitoringList = new LinkedList<>();

        for (ObjectName mbean : remote.queryNames(objectName, null)) {
            Long count = (Long) remote.getAttribute(mbean, ATTRIBUTE);
            if (count > 0) monitoringList.add(new MonitoringEntry(mbean, count));
        }

        return monitoringList;
    }

    private static class MonitoringEntry {
        private ObjectName objectName;
        private long count;

        MonitoringEntry(ObjectName objectName, long count) {
            this.objectName = objectName;
            this.count = count;
        }
    }

}
