package com.company.log2graphite.utils;

import org.apache.log4j.Logger;
import java.util.concurrent.*;

public class AccessMetricHashMap extends ConcurrentHashMap<Long, AccessMetric> {
    private static final Logger LOG = Logger.getLogger(AccessMetricHashMap.class);

    private long lastUploadTime;
    private long lastUpdateTime;

    public void update(AccessMetric m) {
        if (m.getTimestamp() > lastUploadTime) {
            if (containsKey(m.getTimestamp())) {
                get(m.getTimestamp()).update(m);
            } else {
                put(m.getTimestamp(), m);
            }
            lastUpdateTime = System.currentTimeMillis();
        } else {
            LOG.error("got too old metric " + m.getTimestamp());
        }
    }

    public String toString() {
        String s = "";
        for (Long key : keySet()) {
            s += get(key).toString();
        }
        return s;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getLastUploadTime() {
        return lastUploadTime;
    }

    public void setLastUploadTime(long lastUploadTime) {
        this.lastUploadTime = lastUploadTime;
    }
}
