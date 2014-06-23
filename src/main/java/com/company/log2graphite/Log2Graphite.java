package com.company.log2graphite;

import com.company.log2graphite.utils.*;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.concurrent.*;

public class Log2Graphite {
    private static final Logger LOG = Logger.getLogger(Log2Graphite.class);

    private static ArrayBlockingQueue<String> logInputQueue = new ArrayBlockingQueue<String>(10240);
    private static ArrayBlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<AccessMetric>(10240);

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
            if (cli.getOptionNoTail()) {
                LOG.info("read " + cli.getLogPath() + " and exit");
                ExecutorService execParser = Executors.newFixedThreadPool(1);
                execParser.execute(new Reader(cli.getLogPath(), logInputQueue));
            } else {
                LOG.info("tail " + cli.getLogPath());
                Tail tailer = new Tail(cli.getLogPath(), cli.getOptionFromEnd(), logInputQueue);
                tailer.run();
            }

            // run parsers
            AccessMetricParser accessMetricParser = new AccessMetricParser(cli.getLogFormat());
            ExecutorService execParser = Executors.newFixedThreadPool(cli.getParserNumThreads());
            for (int i = 0; i < cli.getParserNumThreads(); i++) {
                execParser.execute(new LogParser(logInputQueue, logInputMetric, accessMetricParser.getLogFormat()));
            }

            // run collector
            try {
                Collector collector = new Collector(logInputMetric, cli.getGraphiteHost(), cli.getAggregateMetricTimeout());
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