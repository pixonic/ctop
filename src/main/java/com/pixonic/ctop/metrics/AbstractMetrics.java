package com.pixonic.ctop.metrics;

import com.pixonic.ctop.StringUtils;
import com.pixonic.ctop.SttySupport;

import javax.management.MBeanServerConnection;
import java.util.Date;
import java.util.Iterator;
import java.util.NavigableSet;

public abstract class AbstractMetrics implements Metrics {

    protected volatile boolean shutdown = false;
    protected final long interval;
    protected final MBeanServerConnection remote;
    protected final String keySpace;
    protected final MetricsMode metricsMode;
    protected final MetricsType metricsType;
    protected final MetricsCollector metricsCollector;

    AbstractMetrics(long interval, MBeanServerConnection remote, String keySpace, MetricsMode metricsMode, MetricsType metricsType, MetricsCollector metricsCollector) {
        this.interval = interval;
        this.remote = remote;
        this.keySpace = keySpace;
        this.metricsMode = metricsMode;
        this.metricsType = metricsType;
        this.metricsCollector = metricsCollector;
    }

    void printMetrics(NavigableSet<ResultItem> readResult, NavigableSet<ResultItem> writeResult) {
        //clear console
        System.out.print("\033[H\033[2J");
        System.out.flush();

        System.out.println("Cassandra top v0.2");
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
        Long totalReadCount = 0L;
        Long totalWriteCount = 0L;
        boolean isMetricsEnabled = !metricsType.equals(MetricsType.NONE);
        for (int i = 7; i < height; i++) {
            if (readIt.hasNext()) {
                ResultItem resultItem = readIt.next();
                if (maxReadCount == null) maxReadCount = resultItem.count;
                totalReadCount += resultItem.count;
                leftStr = formatCounter(resultItem, maxReadCount);


                if (isMetricsEnabled) {
                    String metricString = resultItem.getCf().getKeyProperty("keyspace") + ".table." + resultItem + "." + "read";
                    metricsCollector.getMetricRegistry().counter(metricString).inc(resultItem.count);
                }
            } else {
                leftStr = "";
            }
            if (writeIt.hasNext()) {
                ResultItem resultItem = writeIt.next();
                if (maxWriteCount == null) maxWriteCount = resultItem.count;
                totalWriteCount += resultItem.count;
                rightStr = formatCounter(resultItem, maxWriteCount);

                if (isMetricsEnabled) {
                    String metricString = resultItem.getCf().getKeyProperty("keyspace") + ".table." + resultItem + "." + "write";
                    metricsCollector.getMetricRegistry().counter(metricString).inc(resultItem.count);
                }
            } else {
                rightStr = "";
            }
            if (leftStr.length() == 0 && rightStr.length() == 0) break;
            System.out.println(makeLine(leftStr, rightStr, posWrite));
        }

        if (isMetricsEnabled) {
            metricsCollector.getReadCount().inc(totalReadCount);
            metricsCollector.getWriteCount().inc(totalWriteCount);
        }
    }


    protected void publishMetrics(NavigableSet<ResultItem> readResult, NavigableSet<ResultItem> writeResult) {

    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    protected String formatCounter(ResultItem resultItem, long maxCount) {
        int maxLen = String.valueOf(maxCount).length();

        if (metricsMode.equals(MetricsMode.KEYSPACE)) {
            return StringUtils.leftPad(String.valueOf(resultItem.count), maxLen + 1) + " " + resultItem;
        } else {
            return StringUtils.leftPad(String.valueOf(resultItem.count), maxLen + 1) + " " + keySpace + "." + resultItem;
        }
    }

    protected String makeLine(String left, String right, int rightPos) {
        if (left.length() > rightPos) left = left.substring(0, rightPos);
        if (right.length() > rightPos) left = right.substring(0, rightPos);
        return StringUtils.rightPad(left, rightPos) + " " + right;
    }

}
