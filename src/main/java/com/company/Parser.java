package com.company;

import java.util.concurrent.BlockingQueue;
import java.util.regex.*;

public class Parser implements Runnable {

    private static final int PSLEEP = 500;
    private static BlockingQueue<String> logInputQueue;
    private static BlockingQueue<AccessMetric> logInputMetric;

    private AccessMetric currentMetric;

    private final String logEntryPattern = "([^\\s\"]+|(?:[^\\s\"]*\"[^\"]*\"[^\\s\"]*)+)(?:\\s|$)";
    private Pattern logPattern = Pattern.compile(logEntryPattern);
    private Matcher matcher = logPattern.matcher("");

    public Parser(BlockingQueue<String> q, BlockingQueue<AccessMetric> m) {
        logInputQueue = q;
        logInputMetric = m;
    }

    public void run() {
        int counter = 0;
        int counterPrev = 0;
        Thread thread = Thread.currentThread();
        String currentLine;
        while (true) {
            try {
                currentLine = logInputQueue.take();
                counter++;
                if (counter > (counterPrev + 100000)) {
                    counterPrev = counter;
                    System.out.println(thread.getId() + ": counter " + counter);
                }
                //System.out.println("Process : " + currentLine);
                process(currentLine);
            } catch (InterruptedException m) {
                try {
                    Thread.sleep(PSLEEP);
                } catch (InterruptedException i) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void process(String s) {
        if (parse(s)) {
            push();
        }
    }

    private boolean parse(String s) {
        matcher.reset(s);
        final int MAX_MATCHED_FIELDS = 20;
        String[] matchedField = new String[MAX_MATCHED_FIELDS];

        int matchedFieldCounter = 0;
        while (matcher.find() && (matchedFieldCounter < MAX_MATCHED_FIELDS)) {
            matchedFieldCounter++;
            matchedField[matchedFieldCounter] = matcher.group();
        }
        return readMetrics(s, matchedFieldCounter, matchedField);
    }

    private boolean readMetrics(String s, int c, String[] matchedField) {
        currentMetric = new AccessMetric();
        try {
            if (c == 16) { // access.log with Body
                currentMetric.insertTimestamp(matchedField[4] + matchedField[5]);
                currentMetric.min = Short.parseShort(matchedField[4].substring(16, 18));
                currentMetric.size = Integer.parseInt(matchedField[8].replace(" ", ""));
                currentMetric.request_time = Float.parseFloat(matchedField[14].replace("\"", "").replace(" ", "").equals("-") ? "0" : matchedField[14].replace("\"", ""));
                currentMetric.upstream_time = Float.parseFloat(matchedField[15].replace("\"", "").replace(" ", "").equals("-") ? "0" : matchedField[15].replace("\"", ""));
                currentMetric.methods.insert(matchedField[6]);
                currentMetric.types.insert(matchedField[6]);
                currentMetric.codes.put(Integer.parseInt(matchedField[7].replace(" ", "")), 1L);
                currentMetric.requests = 1;
                return true;
            }
        } catch (Exception m) {
            System.out.println(m + " in : " + s);
        }
        return false;
    }

    private void push() {
        try {
            logInputMetric.put(currentMetric);
        } catch (InterruptedException m) {
            System.out.println(m + " : while adding parsed metric to pool");
        }
    }
}