package com.company.log2graphite;

import org.apache.log4j.Logger;
import java.io.*;
import java.nio.file.Paths;
import java.util.Properties;

public class Props {
    private static final Logger LOG = Logger.getLogger(Props.class);

    private Properties properties = new Properties();

    public Props(String configFile) throws IOException {
        properties.load(Args.class.getClassLoader().getResourceAsStream("conf.properties"));
        if (configFile != null) {
            File file = new File(configFile);
            if (!file.isAbsolute()) {
                configFile = Paths.get(System.getProperty("user.dir"), configFile).toString();
            }
            LOG.info("read property from " + configFile);
            FileInputStream configFileStream = new FileInputStream(configFile);
            properties.load(configFileStream);
            configFileStream.close();
        }
    }

    public String getLogFormat() {
        return properties.getProperty("log_format");
    }
}
