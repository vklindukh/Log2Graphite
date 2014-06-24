package com.company.log2graphite.utils;

import org.apache.log4j.Logger;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Collector {
    private static final Logger LOG = Logger.getLogger(Collector.class);

    private BlockingQueue<AccessMetric> logInputMetric;
    private AccessMetricHashMap outputMetric;

    private static int metricAggregationTimeout;
    private static final int UPLOAD_PERIOD = 10000;
    private static final int INPUT_METRIC_TIMEOUT = 3;
    private static final int INPUT_METRIC_TIMEOUT_NOTIFY = 60000;

    private static boolean uploadToGraphite = false;
    private static String graphiteServer;
    private static final int graphiteServerPort = 2003;
    private static String graphiteMetricBase = "access";

    public Collector (BlockingQueue<AccessMetric> logInputMetric, String s, int metricAggregationTimeout) throws UnknownHostException {
        this.logInputMetric = logInputMetric;
        Collector.metricAggregationTimeout = metricAggregationTimeout;
        if (s != null ) {
            graphiteServer = s;
            try {
                graphiteMetricBase += "." + InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException m) {
                System.out.println(m.getMessage());
                System.exit(255);
            }
            uploadToGraphite = true;
            LOG.info("use Graphite server '" + s + "' for metric upload");
        } else {
            LOG.info("do not upload metric to Graphite");
        }
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
                        boolean uploadStatus = false;
                        while (uploadToGraphite && !uploadStatus) {
                            uploadStatus = toGraphite(outputMetric.get(timestamp));
                        }
                        outputMetric.get(timestamp).setLastUploaded();
                        LOG.info("aggregated metric :" + System.getProperty("line.separator") + outputMetric.get(timestamp));
                    } catch (IOException e) {
                        LOG.error(e + " while sending metric to " + graphiteServer);
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

    private boolean toGraphite(AccessMetric metric) throws IOException {
        SocketAddress address = new InetSocketAddress(graphiteServer, graphiteServerPort);
        Socket clientSocket = new Socket();
        clientSocket.connect(address, 10000);
        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());

        Map<String , String> metricFormatted = metric.format();
        for (String metricName : metricFormatted.keySet()) {
            if (!metricFormatted.get("timestamp").equals("0")) {
                if (!metricName.equals("timestamp")) {
                    outToServer.writeBytes(graphiteMetricBase + "." + metricName + " " +
                            metricFormatted.get(metricName) + " " + metricFormatted.get("timestamp") +
                            System.getProperty("line.separator"));
                }
            }
        }
        clientSocket.close();
        return true;
    }
}
