package com.pixonic.ctop.util;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.pixonic.ctop.metrics.MetricsCollector;
import com.pixonic.ctop.metrics.MetricsType;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MetricUtils {
    public static MetricsCollector registerMetricsReporter(MetricsType metricsType, Properties properties) {

        MetricRegistry metricRegistry = new MetricRegistry();
        MetricsCollector metricsCollector = null;

        switch (metricsType) {
            case GRAPHITE: {
                System.out.println("Registering Graphite Reporter");

                String graphiteHost = properties.getProperty("graphite.host").trim();
                int graphitePort = Integer.parseInt(properties.getProperty("graphite.port", "2003").trim());
                String graphitePrefix = properties.getProperty("graphite.prefix", "ctop").trim();
                int graphiteFreqInterval = Integer.parseInt(properties.getProperty("graphite.freq", "1").trim());

                Graphite graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort));

                GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(metricRegistry)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .prefixedWith(graphitePrefix)
                        .build(graphite);

                graphiteReporter.start(graphiteFreqInterval, TimeUnit.MINUTES);

                metricsCollector = new MetricsCollector(metricRegistry);

                Runtime.getRuntime().addShutdownHook(new Thread(() -> graphiteReporter.stop()));


                break;
            }

            case CONSOLE: {
                ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build();

                consoleReporter.start(1, TimeUnit.SECONDS);

                metricsCollector = new MetricsCollector(metricRegistry);

                Runtime.getRuntime().addShutdownHook(new Thread(() -> consoleReporter.stop()));
                break;
            }

            case NONE:
            default: {
                System.out.println("No reporters registered!");
                break;
            }
        }

        return metricsCollector;
    }
}
