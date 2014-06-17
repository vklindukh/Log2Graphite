package com.company;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import java.text.ParseException;
import java.util.concurrent.BlockingQueue;
import java.util.regex.*;

public class Parser implements Runnable {

    private static BlockingQueue<String> logInputQueue;
    private static BlockingQueue<AccessMetric> logInputMetric;
    private static final Logger LOG = Logger.getLogger(Parser.class);

    final int MAX_MATCHED_FIELDS = 20;
    String[] matchedField = new String[MAX_MATCHED_FIELDS];
    int matchedFieldCounter = 0;

    private AccessMetric currentMetric;

    private final String logEntryPattern = "([^\\s\"]+|(?:[^\\s\"]*\"[^\"]*\"[^\\s\"]*)+)(?:\\s|$)";
    private Pattern logPattern = Pattern.compile(logEntryPattern);
    private Matcher matcher = logPattern.matcher("");

    public Parser(BlockingQueue<String> q, BlockingQueue<AccessMetric> m) {
        logInputQueue = q;
        logInputMetric = m;
    }

    public void run() {
        String currentLine;
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
            if (parse(s)) {
                currentMetric = new AccessMetric();
                if (currentMetric.put(matchedField, matchedFieldCounter)) {
                    push(currentMetric);
                }
            }
        } catch (ParseException m) {
            LOG.error(m + " while parsing : " + s);
        }
    }

    private boolean parse(String s) {
        matcher.reset(s);
        matchedFieldCounter = 0;

        while (matcher.find() && (matchedFieldCounter < MAX_MATCHED_FIELDS)) {
            matchedFieldCounter++;
            matchedField[matchedFieldCounter] = matcher.group();
        }

        return (matchedFieldCounter != 0);
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