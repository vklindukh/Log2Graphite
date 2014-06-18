package com.company;

import org.apache.log4j.Logger;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;

public class Reader implements Runnable {

    private static final Logger LOG = Logger.getLogger(Tail.class);

    private static String logFile = null;
    private static BlockingQueue<String> logInputQueue;

    public Reader(String f, BlockingQueue<String> q) {
        logFile = f;
        logInputQueue = q;
    }

    public void run() {
        try {
            BufferedReader reader;

            LOG.info("started Reader " + Thread.currentThread().getId());
            if (logFile.matches("^.*gz$")) {
                reader = new BufferedReader(new InputStreamReader (new GZIPInputStream(
                        new FileInputStream(logFile))));
            } else {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)));
            }
            String s;
            while ((s = reader.readLine()) != null) {
                logInputQueue.put(s);
            }

            LOG.info("log " + logFile + " processed");
            logInputQueue.put(AccessMetricParser.LOG_FINISHED);
            reader.close();
        } catch (IOException | InterruptedException m) {
            LOG.fatal(m.toString());
            System.exit(255);
        }
    }
}
