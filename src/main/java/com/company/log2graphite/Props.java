package com.company.log2graphite;

import org.apache.log4j.Logger;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Props {
    private static final Logger LOG = Logger.getLogger(Props.class);

    Properties properties = new java.util.Properties();

    public Props(String configFile) throws IOException {
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

            java.util.Properties propertiesOverride = new java.util.Properties();
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
