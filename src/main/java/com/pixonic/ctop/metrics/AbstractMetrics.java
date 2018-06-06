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
    protected final TargetType targetType;
    protected final MetricsType metricsType;
    protected final MetricsCollector metricsCollector;

    AbstractMetrics(long interval, MBeanServerConnection remote, String keySpace, TargetType targetType, MetricsType metricsType, MetricsCollector metricsCollector) {
        this.interval = interval;
        this.remote = remote;
        this.keySpace = keySpace;
        this.targetType = targetType;
        this.metricsType = metricsType;
        this.metricsCollector = metricsCollector;
    }

    void printMetrics(NavigableSet<ResultItem> readResult, NavigableSet<ResultItem> writeResult) {
        boolean isMetricsEnabled = !metricsType.equals(MetricsType.NONE);
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

        if(!isMetricsEnabled) {
            System.out.println(makeLine(leftStr, rightStr, posWrite));
            System.out.println();
        }
        Iterator<ResultItem> readIt = readResult.iterator();
        Iterator<ResultItem> writeIt = writeResult.iterator();
        Long maxReadCount = null, maxWriteCount = null;

        Long totalReadCount = 0L;
        Long totalWriteCount = 0L;


        for (int i = 7; i < height; i++) {
            if (readIt.hasNext()) {
                ResultItem resultItem = readIt.next();
                if (maxReadCount == null) maxReadCount = resultItem.count;
                totalReadCount += resultItem.count;
                leftStr = formatCounter(resultItem, maxReadCount);

                //When graphite or console metrics push is enabled then publish it
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

                //When graphite or console metrics push is enabled then publish it
                if (isMetricsEnabled) {
                    String metricString = resultItem.getCf().getKeyProperty("keyspace") + ".table." + resultItem + "." + "write";
                    metricsCollector.getMetricRegistry().counter(metricString).inc(resultItem.count);
                }
            } else {
                rightStr = "";
            }
            if (leftStr.length() == 0 && rightStr.length() == 0) break;

            //print in console the formatted one when MetricsType is NONE
            if(!isMetricsEnabled) {
                System.out.println(makeLine(leftStr, rightStr, posWrite));
            }
        }

        //When graphite or console metrics push is enabled then publish the total read and write too
        if (isMetricsEnabled) {
            metricsCollector.getReadCount().inc(totalReadCount);
            metricsCollector.getWriteCount().inc(totalWriteCount);
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    protected String formatCounter(ResultItem resultItem, long maxCount) {
        int maxLen = String.valueOf(maxCount).length();

        //when the mode is not a single Keyspace then prefix the keyspace name to the metric to know from which keyspace it is
        if (TargetType.KEYSPACE.equals(targetType)) {
            return StringUtils.leftPad(String.valueOf(resultItem.count), maxLen + 1) + " " + resultItem;
        } else {
            String keyspace = resultItem.getCf().getKeyProperty("keyspace");
            return StringUtils.leftPad(String.valueOf(resultItem.count), maxLen + 1) + " " + keyspace + "." + resultItem;
        }
    }

    protected String makeLine(String left, String right, int rightPos) {
        if (left.length() > rightPos) left = left.substring(0, rightPos);
        if (right.length() > rightPos) left = right.substring(0, rightPos);
        return StringUtils.rightPad(left, rightPos) + " " + right;
    }

}
