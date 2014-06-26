package com.company.log2graphite.core;

import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.Map;

public class Graphite implements MetricReceiver {
    private static final Logger LOG = Logger.getLogger(Graphite.class);

    private boolean senderEnabled = false;
    private SocketAddress address;
    private static String graphiteMetricBase;

    public Graphite(String localHostname, String graphiteServer, int graphiteServerPort) {
        if (graphiteServer != null) {
            address = new InetSocketAddress(graphiteServer, graphiteServerPort);
            if (localHostname == null) {
                throw new IllegalStateException("cannot get local hostname");
            } else {
                graphiteMetricBase = "access." + localHostname + ".";
            }
            LOG.info("upload metric to Graphite server '" + graphiteServer + "'" + " with metric prefix '" + graphiteMetricBase + "'");
            senderEnabled = true;
        } else {
            LOG.info("do not upload metric to Graphite");
        }
    }

    public String getName() {
        return "Graphite";
    }

    public boolean sent(Long timestamp, Map<String , String> metricsPair) throws IOException {
        if (senderEnabled) {
            Socket clientSocket = new Socket();
            clientSocket.connect(address, 10000);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());

            if (timestamp != 0) {
                for (String metricName : metricsPair.keySet()) {
                    outToServer.writeBytes(graphiteMetricBase + metricName + " " +
                            metricsPair.get(metricName) + " " + timestamp.toString() +
                            System.getProperty("line.separator"));
                }
            }
            clientSocket.close();
        }
        return true;
    }
}
