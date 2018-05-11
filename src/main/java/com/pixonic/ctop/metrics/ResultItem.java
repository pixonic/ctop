package com.pixonic.ctop.metrics;

import javax.management.ObjectName;

public class ResultItem implements Comparable<ResultItem> {
    private final ObjectName cf;
    private final String keyProperty;
    final long count;

    ResultItem(ObjectName cf, String keyProperty, long count) {
        this.cf = cf;
        this.keyProperty = keyProperty;
        this.count = count;
    }

    @Override public int compareTo(ResultItem o) {
        long d = count - o.count;
        return -(d < 0 ? -1 : (d > 0 ? 1 : 0));
    }

    @Override public String toString() {
        return cf.getKeyProperty(keyProperty);
    }
}
