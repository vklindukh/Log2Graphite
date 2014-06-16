package com.company;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Collector {

    private static final int CSLEEP = 1000;
    private static final int WAIT_BEFORE_UPLOAD = 120; // upload metric older then 120 seconds

    private static BlockingQueue<AccessMetric> logInputMetric;
    private AccessMetricHashMap outputMetric;
    private String graphiteServer;
    private static final int graphiteServerPort = 2003;
    private static String graphiteMetricBase = "access";

    public Collector (BlockingQueue<AccessMetric> metrics, String s) {
        logInputMetric = metrics;
        outputMetric = new AccessMetricHashMap();
        graphiteServer = s;
        try {
            graphiteMetricBase += "." + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException m) {
            System.out.println(m);
            System.exit(255);
        }
    }

    public void run() {
        AccessMetric metric;
        while (true) {
            try {
                metric = logInputMetric.take();
                outputMetric.update(metric);
                upload();
            } catch (InterruptedException m) {
                try {
                    Thread.sleep(CSLEEP);
                } catch (InterruptedException i) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void upload() {
        if (outputMetric.maxUpdatedTime > (outputMetric.lastUploadTime + WAIT_BEFORE_UPLOAD)) { // upload all <= maxUpdatedTime - 120
            Long newLastUploadTime = 0L;

            for (Long key : outputMetric.keySet()) {
                if (key <= outputMetric.lastUploadTime)
                    outputMetric.remove(key);
                else if (key <= (outputMetric.maxUpdatedTime - WAIT_BEFORE_UPLOAD)) {
                    try {
                        AccessMetric metric = outputMetric.get(key);
                        toGraphite(metric);
                        System.out.println("upload : " + metric);
                        if (key > newLastUploadTime)
                            newLastUploadTime = key;
                        outputMetric.remove(key);
                    } catch (IOException m) {
                        System.out.println(m + " while sending metric to " + graphiteServer);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException i) {
                            Thread.currentThread().interrupt();
                        }
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
