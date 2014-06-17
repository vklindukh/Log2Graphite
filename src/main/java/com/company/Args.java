package com.company;

import org.apache.commons.cli.*;

public class Args {
    private Options options = new Options();
    private CommandLine cmd;

    public Args() {
        optionsInit();
    }

    private void optionsInit() {
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
                .isRequired()
                .withDescription("Graphite host IP")
                .create("h");
        options.addOption(graphiteHost);
    }

    public void parse(String[] args) throws ParseException {
        try {
            CommandLineParser parser = new BasicParser();
            cmd = parser.parse(options, args);
        } catch (ParseException m) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log2Graphite", options);
            throw new ParseException(m.toString());
        }
    }

    public int ParserThreads() {
        String s = (cmd.getOptionValue("t") == null) ? "1" : cmd.getOptionValue("t");
        return Integer.parseInt(s);
    }

    public boolean fromEnd() {
        return (!cmd.hasOption("start"));
    }

    public boolean noTail() {
        return (cmd.hasOption("notail"));
    }
    public String accessLogPath() {
        return cmd.getOptionValue("f");
    }

    public String graphiteHost() {
        return cmd.getOptionValue("h");
    }
}
