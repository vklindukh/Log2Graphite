package com.company.log2graphite;

import com.company.log2graphite.core.*;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.concurrent.*;

public class Log2Graphite {
    private static final Logger LOG = Logger.getLogger(Log2Graphite.class);

    private static BlockingQueue<String> logInputQueue = new ArrayBlockingQueue<>(10240);
    private static BlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<>(10240);

    private static Args cli;
    private static Props properties;

    public static void main(String[] args) {
        LOG.info("started Log2Graphite " + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

        try {
            cli = new Args(args);
            properties = new Props(cli.getConfigFile());
        } catch (ParseException | IOException m) {
            System.err.println(m);
            System.exit(255);
        }

        try {
            if (cli.getOptionNoTail()) {
                LOG.info("read '" + cli.getLogPath() + "' and exit");
                ExecutorService execParser = Executors.newFixedThreadPool(1);
                execParser.execute(new Reader(cli.getLogPath(), cli, logInputQueue));
            } else {
                LOG.info("tail " + cli.getLogPath());
                Tail tailer = new Tail(cli.getLogPath(), cli.getOptionFromEnd(), logInputQueue);
                tailer.run();
            }

            // run parsers
            AccessMetricParser accessMetricParser = new AccessMetricParser(properties.getLogFormat());
            ExecutorService execParser = Executors.newFixedThreadPool(cli.getParserNumThreads());
            for (int i = 0; i < cli.getParserNumThreads(); i++) {
                execParser.execute(new LogParser(logInputQueue, logInputMetric, accessMetricParser.getLogFormat()));
            }

            // run collector
            try {
                MetricReceiver receiver = new Graphite(cli.getHostname(), cli.getGraphiteHost(), cli.getGraphitePort());
                Collector collector = new Collector(logInputMetric, cli.getAggregateMetricTimeout(), receiver);
                collector.run();
            } catch (UnknownHostException | InterruptedException m) {
                m.printStackTrace();
                LOG.fatal(m.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.fatal(e.getMessage());
            System.exit(255);
        }
        System.exit(0); // kill other threads
    }
}