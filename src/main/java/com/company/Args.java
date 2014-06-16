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
    }

    public void parse(String[] args) {
        try {
            CommandLineParser parser = new BasicParser();
            cmd = parser.parse(options, args);
        } catch (ParseException m) {
            System.out.println(m);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log2Graphite", options);
            System.exit(255);
        }
    }

    public int ParserThreads() {
        String s = (cmd.getOptionValue("t") == null) ? "1" : cmd.getOptionValue("t");
        return Integer.parseInt(s);
    }

    public boolean fromEnd() {
        if (cmd.hasOption("start"))
            return false;
        return true;
    }

    public String accessLogPath() {
        return cmd.getOptionValue("f");
    }

    public String graphiteHost() {
        return cmd.getOptionValue("h");
    }
}
