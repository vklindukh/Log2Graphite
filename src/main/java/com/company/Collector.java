package com.company;

import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Collector {
    private static final Logger LOG = Logger.getLogger(Collector.class);

    private static final int WAIT_BEFORE_UPLOAD = 120; // upload metric older then 120 seconds

    private static BlockingQueue<AccessMetric> logInputMetric;
    private AccessMetricHashMap outputMetric;

    private String graphiteServer;
    private static final int graphiteServerPort = 2003;
    private static String graphiteMetricBase = "access";

    public Collector (BlockingQueue<AccessMetric> metrics, String s) throws UnknownHostException {
        logInputMetric = metrics;
        outputMetric = new AccessMetricHashMap();
        graphiteServer = s;
        graphiteMetricBase += "." + InetAddress.getLocalHost().getHostName();
    }

    public void run() throws InterruptedException {
        AccessMetric metric;
        while (true) {
            metric = logInputMetric.take();
            outputMetric.update(metric);
            upload();
        }
    }

    private void upload() throws InterruptedException {
        if (outputMetric.maxUpdatedTime > (outputMetric.lastUploadTime + WAIT_BEFORE_UPLOAD)) { // upload all <= maxUpdatedTime - 120
            Long newLastUploadTime = 0L;

            for (Long key : outputMetric.keySet()) {
                if (key <= outputMetric.lastUploadTime)
                    outputMetric.remove(key);
                else if (key <= (outputMetric.maxUpdatedTime - WAIT_BEFORE_UPLOAD)) {
                    try {
                        AccessMetric metric = outputMetric.get(key);
                        toGraphite(metric);
                        LOG.info("upload :" + System.getProperty("line.separator") + metric);
                        if (key > newLastUploadTime)
                            newLastUploadTime = key;
                        outputMetric.remove(key);
                    } catch (IOException m) {
                        LOG.error(m + " while sending metric to " + graphiteServer);
                        Thread.sleep(500);
                    }
                }
            }

            if (newLastUploadTime > outputMetric.lastUploadTime)
                outputMetric.lastUploadTime = newLastUploadTime;
        }
    }

    private void toGraphite(AccessMetric metric) throws IOException {
        SocketAddress address = new InetSocketAddress(graphiteServer, graphiteServerPort);
        Socket clientSocket = new Socket();
        clientSocket.connect(address, 10000);
        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());

        ConcurrentHashMap<String , String> metricFormatted = metric.format();
        for (String metricName : metricFormatted.keySet()) {
            if (!metricName.equals("timestamp")) {
                outToServer.writeBytes(graphiteMetricBase + "." + metricName + " " +
                        metricFormatted.get(metricName) + " " + metricFormatted.get("timestamp") +
                        System.getProperty("line.separator"));
            }
        }
        clientSocket.close();
    }
}
