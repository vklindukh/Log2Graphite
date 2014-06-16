package com.company;

import org.apache.commons.cli.*;
import java.net.UnknownHostException;
import java.util.concurrent.*;

public class Log2Graphite {

    private static BlockingQueue<String> logInputQueue = new ArrayBlockingQueue<String>(10240);
    private static BlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<AccessMetric>(10240);

    public static void main(String[] args) {
        String logFile = null;
        String graphiteHost = null;
        boolean tailerEnd = true;
        int parserThreads = 1;

        Options options = new Options();

        @SuppressWarnings("all")
        Option inputfile = OptionBuilder.withArgName("filepath")
                .hasArg(true)
                .isRequired()
                .withDescription("log file")
                .create("f");
        options.addOption(inputfile);

        @SuppressWarnings("all")
        Option tailerend = OptionBuilder.withArgName("start")
                .hasArg(false)
                .withDescription("process log from start of file if true. default is false")
                .create("start");
        options.addOption(tailerend);

        @SuppressWarnings("all")
        Option parserthreads = OptionBuilder
                .hasArg(true)
                .withDescription("number of parsers. default is 1 parser)")
                .create("t");
        options.addOption(parserthreads);

        @SuppressWarnings("all")
        Option graphitehost = OptionBuilder.withArgName("host")
                .hasArg(true)
                .isRequired()
                .withDescription("Graphite host IP")
                .create("h");
        options.addOption(graphitehost);

        try {
            CommandLineParser parser = new BasicParser();
            CommandLine cmd = parser.parse(options, args);

            logFile = cmd.getOptionValue("f");
            if (cmd.hasOption("start")) {
                tailerEnd = false;
            }
            graphiteHost = cmd.getOptionValue("h");
            if (cmd.hasOption("t"))
                parserThreads = Integer.parseInt(cmd.getOptionValue("t"));
        } catch (ParseException m) {
            System.out.println(m);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "Log2Graphite", options );
            System.exit(255);
        }

        // run tailer
        Runnable rTailer = new Tail(logFile, tailerEnd, logInputQueue);
        new Thread(rTailer).start();

        // run parsers
        while (parserThreads > 0) {
            Runnable rParser = new Parser(logInputQueue, logInputMetric);
            new Thread(rParser).start();
            parserThreads--;
        }

        // run collector
        Runnable rCollector = null;
        try {
            while (rCollector == null)
            rCollector = new Collector(logInputMetric, graphiteHost);
            new Thread(rCollector).start();
        } catch (UnknownHostException m) {
            System.out.println("error running Collector thread : " + m);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException i) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("Collector started");
    }
}