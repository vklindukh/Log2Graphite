package com.company;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.log4j.Logger;

public class Tail {
    private static final Logger LOG = Logger.getLogger(Tail.class);

    private static String logFile = null;
    private static BlockingQueue<String> logInputQueue;
    private static boolean tailerEnd = false;

    public Tail(String f, boolean e, BlockingQueue<String> q) {
        if (f.matches("^.*gz$")) {
            throw new IllegalStateException("wrong input file extension '" + f + "'");
        }
        logFile = f;
        logInputQueue = q;
        tailerEnd = e;
    }

    public void run() {
        MyListener listener = new MyListener();
        Tailer.create(new File(logFile), listener, 1000, tailerEnd);
    }

    public class MyListener extends TailerListenerAdapter {
        @Override
        public void handle(String line) {
            try {
                logInputQueue.put(line);
            } catch (InterruptedException m) {
               LOG.fatal(m + " : while adding new line to queue");
               System.exit(255);
            }
        }
    }
}

