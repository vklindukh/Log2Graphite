package com.company.log2graphite;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import java.io.*;

public class Args {
    private static final Logger LOG = Logger.getLogger(Args.class);

    private Options options = new Options();
    private CommandLine cmd;

    public Args(String[] args) throws ParseException, IOException {
        optionsInit();
        readOptions(args);
    }

    private void optionsInit() {
        @SuppressWarnings("all")
        Option configFile = OptionBuilder.withArgName("config")
                .hasArg(true)
                .withDescription("config file")
                .create("c");
        options.addOption(configFile);

        @SuppressWarnings("all")
        Option inputFile = OptionBuilder.withArgName("filepath")
                .hasArg(true)
                .isRequired()
                .withDescription("log file")
                .create("f");
        options.addOption(inputFile);

        @SuppressWarnings("all")
        Option noTail = OptionBuilder.withArgName("notail")
                .hasArg(false)
                .withDescription("parse file till end and exit")
                .create("notail");
        options.addOption(noTail);

        @SuppressWarnings("all")
        Option tailerEnd = OptionBuilder.withArgName("start")
                .hasArg(false)
                .withDescription("process log from start of file if true. default is false")
                .create("start");
        options.addOption(tailerEnd);

        @SuppressWarnings("all")
        Option parserThreads = OptionBuilder
                .hasArg(true)
                .withDescription("number of parsers. default is 1 parser)")
                .create("t");
        options.addOption(parserThreads);

        @SuppressWarnings("all")
        Option graphiteHost = OptionBuilder.withArgName("host")
                .hasArg(true)
                .withDescription("Graphite host IP")
                .create("h");
        options.addOption(graphiteHost);

        @SuppressWarnings("all")
        Option aggregateMetricTimeout = OptionBuilder.withArgName("aggregate_time")
                .hasArg(true)
                .withDescription("aggregate metric timeout in seconds. default is 60")
                .create("atime");
        options.addOption(aggregateMetricTimeout);
    }

    public void readOptions(String[] args) throws ParseException {
        try {
            CommandLineParser parser = new BasicParser();
            cmd = parser.parse(options, args);
        } catch (ParseException m) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log2Graphite", options);
            throw new ParseException(m.toString());
        }
    }

    public int getParserNumThreads() {
        String s = (cmd.getOptionValue("t") == null) ? "1" : cmd.getOptionValue("t");
        return Integer.parseInt(s);
    }

    public int getAggregateMetricTimeout() {
        String s = (cmd.getOptionValue("atime") == null) ? "60" : cmd.getOptionValue("atime");
        return Integer.parseInt(s) * 1000;
    }

    public boolean getOptionFromEnd() {
        return (!cmd.hasOption("start"));
    }

    public boolean getOptionNoTail() {
        return (cmd.hasOption("notail"));
    }

    public String getLogPath() {
        return cmd.getOptionValue("f");
    }

    public String getConfigFile() {
        return cmd.getOptionValue("c");
    }

    public String getGraphiteHost() {
        return cmd.getOptionValue("h");
    }
}