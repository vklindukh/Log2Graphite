package com.company;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

public class Tail {
    private static String logFile = null;
    private static BlockingQueue<String> logInputQueue;
    private static boolean tailerEnd = false;

    public Tail(String f, boolean e, BlockingQueue<String> q) {
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
                System.out.println(m + " : while adding new line to queue");
            }
        }
    }
}

