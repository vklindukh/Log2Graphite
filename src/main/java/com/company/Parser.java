package com.company;

import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.util.concurrent.BlockingQueue;

public class Parser implements Runnable {

    private static BlockingQueue<String> logInputQueue;
    private static BlockingQueue<AccessMetric> logInputMetric;
    private static final Logger LOG = Logger.getLogger(Parser.class);

    private AccessMetric currentMetric;

    public Parser(BlockingQueue<String> q, BlockingQueue<AccessMetric> m) {
        logInputQueue = q;
        logInputMetric = m;
    }

    public void run() {
        String currentLine;

        LOG.info("started Parser " + Thread.currentThread().getId());
        while (true) {
            try {
                currentLine = logInputQueue.take();
                process(currentLine);
            } catch (InterruptedException m) {
                LOG.fatal(m);
                System.exit(255);
            }
        }
    }

    private void process(String s) {
        try {
            currentMetric = new AccessMetric();
            if ( currentMetric.parse(s)) {
                push(currentMetric);
            }
        } catch (ParseException m) {
            LOG.error(m + " while parsing : " + s);
        }
    }

    private void push(AccessMetric c) {
        try {
            logInputMetric.put(c);
        } catch (InterruptedException m) {
            LOG.fatal(m + " while pushing metric to queue");
            System.exit(255);
        }
    }
}