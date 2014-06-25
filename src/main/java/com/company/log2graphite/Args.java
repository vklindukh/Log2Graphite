package com.company.log2graphite;

import org.apache.commons.cli.*;
import java.io.*;

public class Args {
    private Options options = new Options();
    private CommandLine cmd;

    public Args(String[] args) throws ParseException, IOException {
        optionsInit();
        readOptions(args);
    }

    private void optionsInit() {
        @SuppressWarnings("all")
        Option configFile = OptionBuilder.withArgName("path to config file")
                .hasArg(true)
                .withDescription("optional")
                .create("c");
        options.addOption(configFile);

        @SuppressWarnings("all")
        Option inputFile = OptionBuilder.withArgName("path to log file")
                .hasArg(true)
                .isRequired()
                .withDescription("required")
                .create("f");
        options.addOption(inputFile);

        @SuppressWarnings("all")
        Option noTail = OptionBuilder.withArgName("")
                .hasArg(false)
                .withDescription("optional: parse single file without tail")
                .create("notail");
        options.addOption(noTail);

        @SuppressWarnings("all")
        Option tailerEnd = OptionBuilder.withArgName("")
                .hasArg(false)
                .withDescription("optional: tail log from start" +
                        System.getProperty("line.separator") + "default is tail log file from end")
                .create("start");
        options.addOption(tailerEnd);

        @SuppressWarnings("all")
        Option parserThreads = OptionBuilder.withArgName("parsers")
                .hasArg(true)
                .withDescription("optional: number of parsers" +
                        System.getProperty("line.separator") + "default is 1 parser")
                .create("t");
        options.addOption(parserThreads);

        @SuppressWarnings("all")
        Option graphiteHost = OptionBuilder.withArgName("host")
                .hasArg(true)
                .withDescription("optional: Graphite host IP")
                .create("h");
        options.addOption(graphiteHost);

        @SuppressWarnings("all")
        Option graphitePort = OptionBuilder.withArgName("port")
                .hasArg(true)
                .withDescription("optional: Graphite TCP port" +
                                System.getProperty("line.separator") + "default is 2003")
                .create("p");
        options.addOption(graphitePort);

        @SuppressWarnings("all")
        Option aggregateMetricTimeout = OptionBuilder.withArgName("seconds")
                .hasArg(true)
                .withDescription("optional: aggregate metric timeout" +
                        System.getProperty("line.separator") + "default is 60")
                .create("atime");
        options.addOption(aggregateMetricTimeout);

        @SuppressWarnings("all")
        Option awsAccessKey = OptionBuilder.withArgName("AWS access key")
                .hasArg(true)
                .withDescription("optional: S3 access key")
                .create("key");
        options.addOption(awsAccessKey);

        @SuppressWarnings("all")
        Option awsSecretKey = OptionBuilder.withArgName("AWS secret key")
                .hasArg(true)
                .withDescription("optional: S3 secret key")
                .create("secret");
        options.addOption(awsSecretKey);
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

    public int getGraphitePort() {
        String s = (cmd.getOptionValue("p") == null) ? "2003" : cmd.getOptionValue("p");
        return Integer.parseInt(s);
    }

    public String getAWSAccessKey() {
        return cmd.getOptionValue("key");
    }

    public String getAWSSecretKey() {
        return cmd.getOptionValue("secret");
    }
}