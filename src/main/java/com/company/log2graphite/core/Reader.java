package com.company.log2graphite.core;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.company.log2graphite.Args;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;

public class Reader implements Runnable {
    private static final Logger LOG = Logger.getLogger(Reader.class);

    private static String logFile = null;
    private static BlockingQueue<String> logInputQueue;
    private static long debugPeriod = System.currentTimeMillis();
    private static Args cli;

    public Reader(String f, Args a, BlockingQueue<String> q) {
        logFile = f;
        logInputQueue = q;
        cli = a;
    }

    public void run() {
        LOG.info("started Reader " + Thread.currentThread().getId());
        try {
            BufferedReader reader = new BufferedReader(openStream());

            String s;
            long linesReceived = 0;
            long timeStarted = System.currentTimeMillis();
            long timeReported = timeStarted;

            while ((s = reader.readLine()) != null) {
                if ((debugPeriod + 10000) < System.currentTimeMillis()) {
                    LOG.debug("current logInputQueue size : " + logInputQueue.size());
                    debugPeriod = System.currentTimeMillis();
                }
                linesReceived++;
                logInputQueue.put(s);
                if ((timeReported + 10000) < System.currentTimeMillis()) {
                    LOG.info("read " + linesReceived + " lines in " +  (System.currentTimeMillis() - timeStarted) / 1000 + " seconds");
                    timeReported = System.currentTimeMillis();
                }
            }

            LOG.info("log " + logFile + " processed");
            LOG.info("total read " + linesReceived + " lines in " +  (System.currentTimeMillis() - timeStarted) / 1000 + " seconds");
            logInputQueue.put(AccessMetricParser.LOG_FINISHED);
            reader.close();
        } catch (IOException | InterruptedException | com.amazonaws.services.s3.model.AmazonS3Exception m) {
            LOG.fatal(m.getStackTrace());
            System.exit(255);
        }
    }

    private InputStreamReader openStream() throws IOException {
        InputStream stream;
        if (logFile.matches("^s3://.*")) {
            stream = openS3Stream()
            ;
        } else {
            stream = new FileInputStream(logFile);
        }

        if (logFile.matches("^.*gz$")) {
             return new InputStreamReader (new GZIPInputStream(stream));
        } else {
            return new InputStreamReader(stream);
        }
    }

    private InputStream openS3Stream() {
        AWSCredentials myCredentials = new BasicAWSCredentials(cli.getAWSAccessKey(), cli.getAWSSecretKey());
        AmazonS3Client s3Client = new AmazonS3Client(myCredentials);
        S3Path path = new S3Path(logFile);
        S3Object object = s3Client.getObject(new GetObjectRequest(path.getBucket(), path.getKey()));

        return object.getObjectContent();
    }

    private class S3Path {
        String s;

        S3Path (String s) {
            if (s.matches("^s3://.*")) {
                this.s = s;
            } else {
                throw new IllegalStateException("wrong S3 path " + s);
            }
        }

        String getBucket() {
            return s.substring(5,s.indexOf("/", 5));
        }

        String getKey() {
            return s.substring(s.indexOf("/", 5) + 1);
        }
    }
}
