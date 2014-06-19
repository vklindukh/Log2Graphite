package com.company;

import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Collector {
    private static final Logger LOG = Logger.getLogger(Collector.class);

    private static BlockingQueue<AccessMetric> logInputMetric;
    private AccessMetricHashMap outputMetric;

    private static final int WAIT_BEFORE_UPLOAD = 120; // upload metric older then 120 seconds
    private String graphiteServer;
    private static final int graphiteServerPort = 2003;
    private static String graphiteMetricBase = "access";

    public Collector (BlockingQueue<AccessMetric> metrics, String s) throws UnknownHostException {
        logInputMetric = metrics;
        outputMetric = new AccessMetricHashMap();
        graphiteServer = s;
        graphiteMetricBase += "." + InetAddress.getLocalHost().getHostName();
    }

//    public void run() throws InterruptedException {
//        AccessMetric metric;
//        while (true) {
//            metric = logInputMetric.take();
//            if (metric.getTimestamp() != 0) {
//                outputMetric.update(metric);
//            } else {
//                Thread.sleep(1000);
//                upload(true);
//                LOG.info("all metric uploaded");
//                System.exit(0);
//            }
//            upload(false);
//        }
//    }
    public void run() throws InterruptedException {
        AccessMetric metric;
        boolean finished = false;
        while (!finished || !logInputMetric.isEmpty()) {
            metric = logInputMetric.take();
            if (metric.getTimestamp() != 0) {
                outputMetric.update(metric);
            } else {
                Thread.sleep(1000);
                finished = true;
            }
            upload(false);
        }
        upload(true);
    }

    private void upload(boolean uploadAll) throws InterruptedException {
        if (uploadAll || (outputMetric.getMaxUpdatedTime() > (outputMetric.getLastUploadTime() + WAIT_BEFORE_UPLOAD))) { // upload all <= maxUpdatedTime - 120
            Long newLastUploadTime = 0L;

            for (Long timestamp : outputMetric.keySet()) {
                if (timestamp <= outputMetric.getLastUploadTime()) {
                    LOG.error("too old metric found (" + timestamp + "). remove");
                    outputMetric.remove(timestamp);
                } else if (uploadAll || (timestamp <= (outputMetric.getMaxUpdatedTime() - WAIT_BEFORE_UPLOAD))) {
                        AccessMetric metric = outputMetric.get(timestamp);
                        boolean uploadStatus = false;

                        while (!uploadStatus) {
                            try {
                                uploadStatus = toGraphite(metric);
                                LOG.info("upload :" + System.getProperty("line.separator") + metric);
                                if (timestamp > newLastUploadTime)
                                    newLastUploadTime = timestamp;
                                outputMetric.remove(timestamp);
                            } catch (IOException m) {
                                LOG.error(m + " while sending metric to " + graphiteServer);
                                Thread.sleep(500);
                            }
                        }
                }
            }

            if (newLastUploadTime > outputMetric.getLastUploadTime())
                outputMetric.setLastUploadTime(newLastUploadTime);
        }
    }

    private boolean toGraphite(AccessMetric metric) throws IOException {
        SocketAddress address = new InetSocketAddress(graphiteServer, graphiteServerPort);
        Socket clientSocket = new Socket();
        clientSocket.connect(address, 10000);
        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());

        ConcurrentHashMap<String , String> metricFormatted = metric.format();
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
