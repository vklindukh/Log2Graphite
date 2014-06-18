package com.company;

import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.util.concurrent.BlockingQueue;

public class Parser implements Runnable {

    private static BlockingQueue<String> logInputQueue;
    private static BlockingQueue<AccessMetric> logInputMetric;
    private String logFormat;
    private AccessMetricParser metricParser = new AccessMetricParser();

    private static final Logger LOG = Logger.getLogger(Parser.class);

    public Parser(BlockingQueue<String> q, BlockingQueue<AccessMetric> m, String f) {
        logInputQueue = q;
        logInputMetric = m;
        logFormat = f;
    }

    public void run() {
        String currentLine;


        LOG.info("started Parser " + Thread.currentThread().getId());
        //currentMetric.prepare(logFormat);
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
            push(metricParser.parse(s));

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