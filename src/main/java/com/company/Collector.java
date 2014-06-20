package com.company;

import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Collector {
    private static final Logger LOG = Logger.getLogger(Collector.class);

    private static BlockingQueue<AccessMetric> logInputMetric;
    private AccessMetricHashMap outputMetric;

    private static final int WAIT_BEFORE_REMOVE = 60000;
    private static final int UPLOAD_PERIOD = 10000;
    private static final int INPUT_METRIC_TIMEOUT = 3;
    private static final int INPUT_METRIC_TIMEOUT_NOTIFY = 100;

    private boolean uploadToGraphite = false;
    private String graphiteServer;
    private static final int graphiteServerPort = 2003;
    private static String graphiteMetricBase = "access";

    public Collector (BlockingQueue<AccessMetric> metrics, String s) throws UnknownHostException {
        logInputMetric = metrics;
        outputMetric = new AccessMetricHashMap();
        if (s != null ) {
            graphiteServer = s;
            uploadToGraphite = true;
            LOG.info("use Graphite server '" + s + "' for metric upload");
        } else {
            LOG.info("do not upload metric to Graphite");
        }
        graphiteMetricBase += "." + InetAddress.getLocalHost().getHostName();
    }

    public void run() throws InterruptedException {
        AccessMetric metric;
        boolean finished = false;
        int timeoutCounter = 0;
        long uploadCounter = System.currentTimeMillis();
        while (!finished || !logInputMetric.isEmpty()) {
            metric = logInputMetric.poll(INPUT_METRIC_TIMEOUT, TimeUnit.SECONDS);
            //LOG.debug("metric : " + metric);
            if (metric != null) {
                if (metric.getTimestamp() != 0) {
                    if (outputMetric.getLastUploadTime() < metric.getTimestamp()) {
                        outputMetric.update(metric);
                    }
                } else {
                    Thread.sleep(1000);
                    finished = true;
                }
            } else {
                timeoutCounter++;
                if (timeoutCounter > INPUT_METRIC_TIMEOUT_NOTIFY) {
                    LOG.info("no parsed data last " + INPUT_METRIC_TIMEOUT + " seconds");
                    timeoutCounter = 0;
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
                LOG.error("too old metric found (" + timestamp + "). remove");
                outputMetric.remove(timestamp);
            } else {
                if (uploadAll ||
                        ((outputMetric.get(timestamp).getLastUploaded() == 0) &&
                                ((outputMetric.get(timestamp).getLastUpdated() + 3000) < System.currentTimeMillis())) ||
                        ((outputMetric.get(timestamp).getLastUploaded() != 0) &&
                                (outputMetric.get(timestamp).getLastUploaded() < outputMetric.get(timestamp).getLastUpdated()))
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
                        (outputMetric.get(timestamp).getLastUpdated() + WAIT_BEFORE_REMOVE) < System.currentTimeMillis()) {
                    outputMetric.remove(timestamp);
                    outputMetric.setLastUploadTime(timestamp);
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
