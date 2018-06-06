package com.pixonic.ctop.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class MetricsCollector {
    private final Counter readCount;
    private final Counter writeCount;

    private final MetricRegistry metricRegistry;

    public Counter getReadCount() {
        return readCount;
    }

    public Counter getWriteCount() {
        return writeCount;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public MetricsCollector(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;

        this.readCount = this.metricRegistry.counter(MetricRegistry.name(getClass(), "totalRead"));
        this.writeCount = this.metricRegistry.counter(MetricRegistry.name(getClass(), "totalWrite"));
    }
}
