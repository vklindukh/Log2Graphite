package com.company.log2graphite;

import com.company.log2graphite.core.AccessMetric;
import com.company.log2graphite.core.AccessMetricParser;
import com.company.log2graphite.core.LogParser;
import java.util.HashMap;
import java.util.concurrent.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class ParserTest {

    private static ArrayBlockingQueue<String> logInputQueue = new ArrayBlockingQueue<>(10);
    private static ArrayBlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<>(10);

    @org.junit.Test
    public void parse() throws Exception {
        AccessMetricParser metricParser = new AccessMetricParser("'$remote_addr - $remote_user [$time_local] \"$request\" '" +
                "           '$status $body_bytes_sent \"$request_body\" '" +
                "           '\"$connection_requests\" \"$http_connection\" '" +
                "           '\"$http_user_agent\" \"$http_x_forwarded_for\" \"$request_time\" \"$upstream_response_time\" \"$pipe\"';");


        ExecutorService execParser = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 2; i++) {
            execParser.execute(new LogParser(logInputQueue, logInputMetric, metricParser.getLogFormat()));
        }

        String s = "68.171.238.156 - - [12/Jun/2014:03:37:02 +0000] \"POST /adserver/ad?uid=0OiOYJKNQ4dIKS_15AxmApjZ6L9_jI3685vOmHxmviw.&test=0&type=SPAU HTTP/1.1\" 204 15 \"{\\x22app\\x22:\\x22bbm\\x22,\\x22appver\\x22:\\x222.2.0.28\\x22,\\x22blocked\\x22:[],\\x22channels\\x22:[\\x22C00321099\\x22],\\x22demo\\x22:[\\x22BBMVersion/2.2\\x22,\\x22DeviceType/GT-I9500\\x22,\\x22OSVersion/Android 4.4 KitKat\\x22,\\x22Manufacturer/SAMSUNG\\x22,\\x22Platform/Android\\x22,\\x22DOB/1991\\x22,\\x22Country/ID\\x22,\\x22realage/22\\x22,\\x22Age/18-24\\x22],\\x22device\\x22:{\\x22platform\\x22:\\x22Android\\x22,\\x22os\\x22:\\x224.4.2.0\\x22,\\x22time\\x22:\\x22Thu, 12 Jun 2014 10:38:56 +0700\\x22,\\x22mcc\\x22:\\x22510\\x22,\\x22device\\x22:\\x22GT-I9500\\x22,\\x22sd\\x22:\\x223\\x22,\\x22carrier\\x22:\\x22Indosat\\x22,\\x22mnc\\x22:\\x221\\x22,\\x22mf\\x22:\\x22samsung\\x22,\\x22sh\\x22:\\x22640\\x22,\\x22sw\\x22:\\x22360\\x22},\\x22tastes\\x22:[\\x22Technology & Computing/Internet Technology\\x22]}\" \"1\" \"Keep-Alive\" \"bbmcore\" \"114.4.23.89, 192.168.1.89\" \"0.002\" \"0.002\" \".\"";
        logInputQueue.put(s);
        logInputQueue.put(s);
        logInputQueue.put(AccessMetricParser.LOG_FINISHED);
        Thread.sleep(2000);

        int checkQueueAttempts = 4;
        AccessMetric metric;
        HashMap<String , String> metricFormatted;
        while (checkQueueAttempts > 0) {
            metric = logInputMetric.poll(10, TimeUnit.SECONDS);
            metricFormatted = metric.format();

            if (metric.getTimestamp() == 0) {
                assertEquals(0, metricFormatted.size());
            } else {
                assertEquals(7, metricFormatted.size());
                assertEquals(1L, Long.parseLong(metricFormatted.get("requests")));
                assertEquals(15L, Long.parseLong(metricFormatted.get("size")));
                assertEquals(0.002, Double.parseDouble(metricFormatted.get("request_time")), 0.0001);
                assertEquals(0.002, Double.parseDouble(metricFormatted.get("upstream_time")), 0.0001);
                assertEquals(1L, Long.parseLong(metricFormatted.get("POST")));
                assertEquals(1L, Long.parseLong(metricFormatted.get("ad")));
                assertEquals(1L, Long.parseLong(metricFormatted.get("204")));
            }
            checkQueueAttempts--;
        }

        assertNull(logInputMetric.poll(1, TimeUnit.SECONDS));
        Thread.sleep(1000);
        assertEquals(0, ((ThreadPoolExecutor) execParser).getActiveCount());
    }
}