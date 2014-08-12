package com.company.log2graphite;

import org.apache.log4j.Logger;
import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Props {
    private static final Logger LOG = Logger.getLogger(Props.class);

    private Properties properties = new Properties();

    public Props(String configFile) throws IOException {
        Properties propertiesFile = new Properties();
        propertiesFile.load(Args.class.getClassLoader().getResourceAsStream("conf-default.properties"));
        properties.putAll(propertiesFile);
        if (configFile != null) {
            File file = new File(configFile);
            if (!file.isAbsolute()) {
                configFile = Paths.get(System.getProperty("user.dir"), configFile).toString();
            }
            LOG.info("read properties from " + configFile);
            FileInputStream configFileStream = new FileInputStream(configFile);
            propertiesFile.load(configFileStream);
            configFileStream.close();
            properties.putAll(propertiesFile);
        }
    }

    public String getLogFormat() {
        return properties.getProperty("log_format");
    }

    public String getAllowedRequests() {
        return properties.getProperty("requests_allowed");
    }
}
