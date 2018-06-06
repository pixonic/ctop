package com.pixonic.ctop.metrics;

import javax.management.MBeanServerConnection;

public class MetricsFactory {

    public static Metrics getMetrics(int version, long interval, MBeanServerConnection remote, String keySpace, TargetType targetType, MetricsType metricsType, MetricsCollector metricsCollector) {
        if (version >= 3) {
            return new CurrentMetrics(interval, remote, keySpace, targetType, metricsType, metricsCollector);
        } else {
            return new OldMetrics(interval, remote, keySpace, targetType, metricsType, metricsCollector);
        }
    }
}
