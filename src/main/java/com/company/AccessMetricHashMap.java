package com.company;

import java.util.HashMap;
import java.util.concurrent.*;

public class AccessMetricHashMap extends ConcurrentHashMap<Long, AccessMetric> {

    public long maxUpdatedTime = 0;
    public long lastUploadTime = 0;

    public void update(AccessMetric m) {
        if (containsKey(m.timestamp)) { // update  metric
            AccessMetric n = get(m.timestamp);
            n.requests += m.requests;
            n.size += m.size;
            n.request_time += m.request_time;
            n.upstream_time += m.upstream_time;
            n.methods.update(m.methods);
            n.types.update(m.types);
            n.codes.update(m.codes);
        } else
            put(m.timestamp, m);
        if (m.timestamp > maxUpdatedTime)
            maxUpdatedTime = m.timestamp;
    }

    public String toString() {
        String s = "";
        for (Long key : keySet()) {
            s += get(key).toString();
        }
        return s;
    }
}
