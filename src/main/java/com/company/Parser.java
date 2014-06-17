package com.company;

import java.util.concurrent.BlockingQueue;
import java.util.regex.*;

public class Parser implements Runnable {

    private static BlockingQueue<String> logInputQueue;
    private static BlockingQueue<AccessMetric> logInputMetric;

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
                System.out.println(m);
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
        } catch (Exception m) {
            System.out.println(m + " while parsing : " + s);
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
            System.out.println(m + " : while pushing metric to queue");
            System.exit(255);
        }
    }
}