package com.company;

import org.apache.log4j.Logger;
import java.io.*;
import java.util.concurrent.BlockingQueue;

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
            FileInputStream stream = new FileInputStream(logFile);
            BufferedReader buffer = new BufferedReader(new InputStreamReader(stream));

            String s;
            while ((s = buffer.readLine()) != null) {
                logInputQueue.put(s);
            }

            LOG.info("log " + logFile + " processed");
            logInputQueue.put("__FINISH__");
            buffer.close();
        } catch (IOException | InterruptedException m) {
            LOG.fatal(m.toString());
            System.exit(255);
        }
    }
}
