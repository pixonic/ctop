package com.pixonic.ctop.metrics;

import javax.management.MBeanServerConnection;

public class MetricsFactory {

    public static Metrics getMetrics(int version, long interval, MBeanServerConnection remote, String keySpace, MetricsMode metricsMode, MetricsType metricsType, MetricsCollector metricsCollector) {
        if (version >= 3) {
            return new CurrentMetrics(interval, remote, keySpace, metricsMode, metricsType, metricsCollector);
        } else {
            return new OldMetrics(interval, remote, keySpace, metricsMode, metricsType, metricsCollector);
        }
    }

}
