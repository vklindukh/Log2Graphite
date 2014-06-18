package com.company;

import org.apache.commons.cli.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import sun.jvm.hotspot.utilities.WorkerThread;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.concurrent.*;

public class Log2Graphite {

    private static final Logger LOG = Logger.getLogger(Log2Graphite.class);

    private static BlockingQueue<String> logInputQueue = new ArrayBlockingQueue<String>(10240);
    private static BlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<AccessMetric>(1024);

    private static Args cli;

    public static void main(String[] args) {
        LOG.info("started Log2Graphite " + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

        try {
            cli = new Args(args);
        } catch (ParseException | IOException m) {
            System.err.println(m);
            System.exit(255);
        }

        try {
            if (cli.noTail()) {
                LOG.info("read " + cli.accessLogPath() + " and exit");
                ExecutorService execParser = Executors.newFixedThreadPool(1);
                execParser.execute(new Reader(cli.accessLogPath(), logInputQueue));
            } else {
                LOG.info("tail " + cli.accessLogPath());
                Tail tailer = new Tail(cli.accessLogPath(), cli.fromEnd(), logInputQueue);
                tailer.run();
            }

            // run parsers
            ExecutorService execParser = Executors.newFixedThreadPool(cli.ParserThreads());
            for (int i = 0; i < cli.ParserThreads(); i++) {
                execParser.execute(new Parser(logInputQueue, logInputMetric, cli.getLogFormat()));
            }

            // run collector
            try {
                Collector collector = new Collector(logInputMetric, cli.graphiteHost());
                collector.run();
            } catch (UnknownHostException | InterruptedException m) {
                LOG.fatal(m);
            }
        } catch (Exception e) {
            LOG.fatal(e.toString());
            System.exit(255);
        }
        System.exit(0); // kill other threads
    }
}