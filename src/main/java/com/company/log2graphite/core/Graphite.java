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

    public Graphite (String graphiteServer, int graphiteServerPort) {
        if (graphiteServer != null) {
            try {
            address = new InetSocketAddress(graphiteServer, graphiteServerPort);
                String hostname = InetAddress.getLocalHost().getHostName();
                if (hostname != null) {
                    graphiteMetricBase = "access." + hostname;
                } else {
                    graphiteMetricBase = "access";
                }
                LOG.info("upload metric to Graphite server '" + graphiteServer + "'" + " with metric prefix '" + graphiteMetricBase + "'");
            } catch (UnknownHostException m) {
                System.err.println(m.getMessage());
                System.exit(255);
            }
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
                    outToServer.writeBytes(graphiteMetricBase + "." + metricName + " " +
                            metricsPair.get(metricName) + " " + timestamp.toString() +
                            System.getProperty("line.separator"));
                }
            }
            clientSocket.close();
        }
        return true;
    }
}
