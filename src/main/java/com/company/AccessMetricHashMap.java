package com.company;

import java.util.concurrent.*;

public class AccessMetricHashMap extends ConcurrentHashMap<Long, AccessMetric> {

    private long lastUploadTime = 0;

    public void update(AccessMetric m) {
        if (containsKey(m.getTimestamp())) {
            get(m.getTimestamp()).update(m);
        } else {
            put(m.getTimestamp(), m);
        }
    }

    public String toString() {
        String s = "";
        for (Long key : keySet()) {
            s += get(key).toString();
        }
        return s;
    }

    public long getLastUploadTime() {
        return lastUploadTime;
    }

    public void setLastUploadTime(long lastUploadTime) {
        this.lastUploadTime = lastUploadTime;
    }
}
