package com.company;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class Args {
    private static final Logger LOG = Logger.getLogger(Args.class);

    private Options options = new Options();
    private CommandLine cmd;
    Properties properties = new Properties();

    public Args(String[] args) throws ParseException, IOException {
        optionsInit();
        readOptions(args);
        readProperties(getConfigFile());
    }

    private void optionsInit() {
        @SuppressWarnings("all")
        Option configFile = OptionBuilder.withArgName("c")
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
        Option noGraphite = OptionBuilder.withArgName("nographite")
                .hasArg(false)
                .withDescription("do not sent data to Graphite")
                .create("nographite");
        options.addOption(noGraphite);

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

    public boolean getOptionFromEnd() {
        return (!cmd.hasOption("start"));
    }

    public boolean getOptionNoTail() {
        return (cmd.hasOption("notail"));
    }

    public boolean getOptionNoGraphite() {
        return (cmd.hasOption("nographite"));
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

    public void readProperties(String configFile) throws IOException {
        properties.load(Args.class.getClassLoader().getResourceAsStream("conf.properties"));
        if (configFile != null) {
            File file = new File(configFile);
            if (!file.isAbsolute()) {
                configFile = System.getProperty("user.dir") + "/" + configFile;
            }
            LOG.info("read property from " + configFile);

            if (!file.exists()) {
                LOG.fatal(configFile + " not exist");
                throw new FileNotFoundException(configFile + " not exist");
            }

            Properties propertiesOverride = new Properties();
            FileInputStream configFileStream = new FileInputStream(file);
            propertiesOverride.load(configFileStream);
            if (propertiesOverride.containsKey("log_format")) {
                properties.setProperty("log_format", propertiesOverride.getProperty("log_format"));
            }
            configFileStream.close();
        }
    }

    public String getLogFormat() {
        return properties.getProperty("log_format");
    }
}
