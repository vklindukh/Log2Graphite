package com.company;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

public class Tail implements Runnable {

    private static final int SLEEP = 1000;
    private static final int QSLEEP = 500;

    private static String logFile = null;
    private static BlockingQueue<String> logInputQueue;
    private static boolean tailerEnd = false;

    public Tail(String f, boolean e, BlockingQueue<String> q) {
        logFile = f;
        logInputQueue = q;
        tailerEnd = e;
    }

    public void run() {
        while (true) {
            try {
                read();
            } catch (IOException e) { // cannot open file
                System.out.println(e);
                System.exit(255);
            }
        }
    }

    private void read() throws IOException {
        MyListener listener = new MyListener();
        Tailer tailer = Tailer.create(new File(logFile), listener, SLEEP, tailerEnd);
        while (true) {
            try {
                Thread.sleep(SLEEP);
            } catch (InterruptedException i) {
                Thread.currentThread().interrupt();
                break;
            }
        }
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

