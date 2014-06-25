package com.company.log2graphite.utils;

import java.io.IOException;
import java.util.Map;

public interface MetricReceiver {
    boolean sent(Long timestamp, Map<String , String> metricsPair) throws IOException;
    String getName();
}