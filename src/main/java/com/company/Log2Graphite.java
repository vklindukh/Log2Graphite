package com.company;

import org.apache.commons.cli.*;
import java.net.UnknownHostException;
import java.util.concurrent.*;

public class Log2Graphite {

    private static BlockingQueue<String> logInputQueue = new ArrayBlockingQueue<String>(10240);
    private static BlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<AccessMetric>(1024);

    public static void main(String[] args) {
        Args cli = new Args();
        cli.parse(args);

        // run tailer
        Tail tailer = new Tail(cli.accessLogPath(), cli.fromEnd(), logInputQueue);
        tailer.run();

        // run parsers
        ExecutorService execParser = Executors.newFixedThreadPool(cli.ParserThreads());
        execParser.execute(new Parser(logInputQueue, logInputMetric));

        // run collector
        Collector collector = new Collector(logInputMetric, cli.graphiteHost());
        collector.run();
    }
}