package com.company.log2graphite.core;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Collector {
    private static final Logger LOG = Logger.getLogger(Collector.class);

    private BlockingQueue<AccessMetric> logInputMetric;
    private AccessMetricHashMap outputMetric;
    private static MetricReceiver receiver;

    private static int metricAggregationTimeout;
    private static final int UPLOAD_PERIOD = 10000;
    private static final int INPUT_METRIC_TIMEOUT = 3;
    private static final int INPUT_METRIC_TIMEOUT_NOTIFY = 60000;

    public Collector (BlockingQueue<AccessMetric> logInputMetric, int metricAggregationTimeout, MetricReceiver receiver) throws UnknownHostException {
        this.logInputMetric = logInputMetric;
        Collector.receiver = receiver;
        Collector.metricAggregationTimeout = metricAggregationTimeout;
        outputMetric = new AccessMetricHashMap();
    }

    public void run() throws InterruptedException {
        AccessMetric metric;
        boolean finished = false;
        long timeoutWaitingMetric = System.currentTimeMillis();
        long uploadCounter = System.currentTimeMillis();

        while (!finished || !logInputMetric.isEmpty()) {
            metric = logInputMetric.poll(INPUT_METRIC_TIMEOUT, TimeUnit.SECONDS);
            if (metric != null) {
                timeoutWaitingMetric = System.currentTimeMillis();
                if (metric.getTimestamp() != 0) {
                    outputMetric.update(metric);
                } else {
                    Thread.sleep(1000);
                    finished = true;
                }
            } else {
                if ((timeoutWaitingMetric + INPUT_METRIC_TIMEOUT_NOTIFY) < System.currentTimeMillis()) {
                    LOG.warn("no parsed data last " + INPUT_METRIC_TIMEOUT_NOTIFY / 1000 + " seconds");
                    timeoutWaitingMetric = System.currentTimeMillis();
                }
            }
            if ((uploadCounter + UPLOAD_PERIOD) < System.currentTimeMillis()) {
                upload(false);
                uploadCounter = System.currentTimeMillis();
            }
        }
        upload(true);
    }

    private void upload(boolean uploadAll) throws InterruptedException {
        for (Long timestamp : outputMetric.keySet()) {
            if (timestamp <= outputMetric.getLastUploadTime()) {
                LOG.debug("aggregated metric " + timestamp + " expired");
                outputMetric.remove(timestamp);
            } else {
                LOG.debug("timestamp getLastUploaded getLastUpdated : " + timestamp + " " +
                        outputMetric.get(timestamp).getLastUploaded() + " " + outputMetric.get(timestamp).getLastUpdated());
                if (uploadAll ||
                        ((outputMetric.get(timestamp).getLastUploaded() == 0) &&
                                ((outputMetric.get(timestamp).getLastUpdated() + 5000) < System.currentTimeMillis())) ||
                        ((outputMetric.get(timestamp).getLastUploaded() != 0) &&
                                ((outputMetric.get(timestamp).getLastUpdated() > outputMetric.get(timestamp).getLastUploaded())) &&
                                (outputMetric.get(timestamp).getLastUploaded() + 5000 < System.currentTimeMillis()))
                        ) {
                    try {
                        Map<String , String> metricFormatted = outputMetric.get(timestamp).format();
                        boolean uploadStatus = false;

                        while (!uploadStatus) {
                            uploadStatus = receiver.sent(timestamp, metricFormatted);
                        }

                        outputMetric.get(timestamp).setLastUploaded();
                        LOG.info("aggregated metric :" + System.getProperty("line.separator") + outputMetric.get(timestamp));
                    } catch (IOException e) {
                        LOG.error(e.getMessage() + " while sending metric to " + receiver.getName());
                        Thread.sleep(500);
                    }
                }
                if ((outputMetric.get(timestamp).getLastUploaded() != 0) &&
                        (outputMetric.get(timestamp).getLastUpdated() + metricAggregationTimeout) < outputMetric.getLastUpdateTime()) {
                    LOG.debug("aggregated metric " + timestamp + " expired");
                    outputMetric.setLastUploadTime(timestamp);
                    outputMetric.remove(timestamp);
                }
            }
        }
    }
}