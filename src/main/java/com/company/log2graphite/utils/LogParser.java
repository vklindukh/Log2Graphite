package com.company.log2graphite.utils;

import org.apache.log4j.Logger;
import java.text.ParseException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public class LogParser implements Runnable {
    private static final Logger LOG = Logger.getLogger(LogParser.class);

    private BlockingQueue<String> logInputQueue;
    private BlockingQueue<AccessMetric> logInputMetric;
    private AccessMetricParser metricParser;
    private AccessMetric aggregatedMetric = new AccessMetric();
    private long aggregatedMetricTimestamp = 0;
    private long queueUpdateLastTime = 0;

    public LogParser(BlockingQueue<String> q, BlockingQueue<AccessMetric> m, HashMap<String, Integer> logFormat) throws ParseException {
        logInputQueue = q;
        logInputMetric = m;
        metricParser = new AccessMetricParser(logFormat);
    }

    public void run() {
        String currentLine;

        LOG.info("started Parser " + Thread.currentThread().getId());
        boolean threadIsActive = true;
        while (threadIsActive) {
            try {
                currentLine = logInputQueue.take();
                threadIsActive = process(currentLine);
            } catch (InterruptedException m) {
                LOG.fatal(m);
                System.exit(255);
            }
        }
        LOG.info("Parser " + Thread.currentThread().getId() + " finished");
    }

    private boolean process(String s) {
        try {
            AccessMetric currentMetric = metricParser.parse(s);
            long currentMetricTimestamp = currentMetric.getTimestamp();

            if (aggregatedMetricTimestamp == 0) {
                aggregatedMetric.forceUpdate(currentMetric);
                aggregatedMetricTimestamp = currentMetricTimestamp;
                if (currentMetricTimestamp == 0) {
                    queueUpdateLastTime = 0;
                }
            } else if ((currentMetricTimestamp == aggregatedMetricTimestamp)) {
                aggregatedMetric.update(currentMetric);
            } else {
                push(aggregatedMetric);
                aggregatedMetric = new AccessMetric();
                aggregatedMetric.forceUpdate(currentMetric);
                aggregatedMetricTimestamp = currentMetricTimestamp;
                if (currentMetricTimestamp == 0) {
                    queueUpdateLastTime = 0;
                }
            }

            if ((queueUpdateLastTime + 1000) < System.currentTimeMillis()) {
                boolean pushStatus = push(aggregatedMetric);
                aggregatedMetric = new AccessMetric();
                aggregatedMetricTimestamp = 0;
                return (pushStatus);
            }
        } catch (ParseException m) {
            LOG.error(m + " while parsing : " + s);
        }
        return true;
    }

    private boolean push(AccessMetric metric) {
        try {
            logInputMetric.put(metric);
            queueUpdateLastTime = System.currentTimeMillis();
            if (metric.getTimestamp() == 0) {
                logInputQueue.put(AccessMetricParser.LOG_FINISHED);
                return false;
            }
        } catch (InterruptedException e) {
            LOG.fatal(e + " while pushing metric to queue");
            System.exit(255);
        }
        return true;
    }
}