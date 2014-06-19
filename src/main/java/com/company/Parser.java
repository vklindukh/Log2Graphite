package com.company;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

public class Parser implements Runnable {
    private static final Logger LOG = Logger.getLogger(Parser.class);

    private static ArrayBlockingQueue<String> logInputQueue;
    private static ArrayBlockingQueue<AccessMetric> logInputMetric;
    private AccessMetricParser metricParser;


    public Parser(ArrayBlockingQueue<String> q, ArrayBlockingQueue<AccessMetric> m, HashMap<String, Integer> logFormat) throws ParseException {
        logInputQueue = q;
        logInputMetric = m;
        metricParser = new AccessMetricParser(logFormat);
    }

    public void run() {
        String currentLine;

        LOG.info("started Parser " + Thread.currentThread().getId());
        //currentMetric.prepare(logFormat);
        boolean threadIsActive = true;
        while (threadIsActive) {
            try {
                currentLine = logInputQueue.take();
                threadIsActive = process(currentLine);
            } catch (InterruptedException m) {
                LOG.fatal(m);
                System.exit(255);
            }
        }
        LOG.info("Parser " + Thread.currentThread().getId() + " finished");
    }

    private boolean process(String s) {
        try {
            return (push(metricParser.parse(s)));
        } catch (ParseException m) {
            LOG.error(m + " while parsing : " + s);
        }
        return true;
    }

    private boolean push(AccessMetric c) {
        try {
            logInputMetric.put(c);
            if (c.getTimestamp() == 0) {
                logInputQueue.put(AccessMetricParser.LOG_FINISHED);
                return false;
            }
        } catch (InterruptedException m) {
            LOG.fatal(m + " while pushing metric to queue");
            System.exit(255);
        }
        return true;
    }
}