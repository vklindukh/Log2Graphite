package com.company;

import org.junit.Test;

import java.text.ParseException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AccessMetricParserTest {


    String s = null;
    @Test(expected = IllegalStateException.class)
    public void parsePropertiesWithException1() throws ParseException {
        String s = null;
        new AccessMetricParser(s);
    }

    @Test(expected = IllegalStateException.class)
    public void parsePropertiesWithException2() throws ParseException {
        new AccessMetricParser("");
    }

    @Test(expected = ParseException.class)
    public void parsePropertiesWithException3() throws ParseException {
        new AccessMetricParser("AA");
    }

    @Test
    public void testParse() throws Exception {
        AccessMetricParser metricParser = new AccessMetricParser("'$remote_addr - $remote_user [$time_local] \"$request\" '" +
                "           '$status $body_bytes_sent \"$request_body\" '" +
                "           '\"$connection_requests\" \"$http_connection\" '" +
                "           '\"$http_user_agent\" \"$http_x_forwarded_for\" \"$request_time\" \"$upstream_response_time\" \"$pipe\"';");

        HashMap<String, Integer> logFormat = metricParser.getLogFormat();
        int value = logFormat.get("fields");
        assertEquals(16, value);
        value = logFormat.get("timestamp");
        assertEquals(3, value);
        value = logFormat.get("size");
        assertEquals(7, value);
        value = logFormat.get("request");
        assertEquals(5, value);
        value = logFormat.get("request_time");
        assertEquals(13, value);
        value = logFormat.get("upstream_time");
        assertEquals(14, value);
        value = logFormat.get("code");
        assertEquals(6, value);

    }

    @Test(expected = ParseException.class)
    public void testNoRequiredField() throws ParseException {
        new AccessMetricParser("'$remote_addr - $remote_user \"$request\" '" +
                "           '$status $body_bytes_sent \"$request_body\" '" +
                "           '\"$connection_requests\" \"$http_connection\" '" +
                "           '\"$http_user_agent\" \"$http_x_forwarded_for\" \"$request_time\" \"$upstream_response_time\" \"$pipe\"';");

    }

    @Test
    public void testMetricParseLimitedMetrics1() throws Exception {
        AccessMetricParser metricParser = new AccessMetricParser("'$remote_addr - $remote_user [$time_local] \"$request\" '" +
                "           '$status $body_bytes_sent \"$request_body\" '" +
                "           '\"$connection_requests\" \"$http_connection\" '" +
                "           '\"$http_user_agent\" \"$http_x_forwarded_for\"");

        HashMap<String, Integer> logFormat = metricParser.getLogFormat();
        int value = logFormat.get("fields");
        assertEquals(13, value);
        value = logFormat.get("timestamp");
        assertEquals(3, value);
        value = logFormat.get("size");
        assertEquals(7, value);
        value = logFormat.get("request");
        assertEquals(5, value);
        value = logFormat.get("code");
        assertEquals(6, value);

        String s = "68.171.238.156 - - [12/Jun/2014:03:37:02 +0000] \"POST /adserver/ad?uid=0OiOYJKNQ4dIKS_15AxmApjZ6L9_jI3685vOmHxmviw.&test=0&type=SPAU HTTP/1.1\" 204 15 \"{\\x22app\\x22:\\x22bbm\\x22,\\x22appver\\x22:\\x222.2.0.28\\x22,\\x22blocked\\x22:[],\\x22channels\\x22:[\\x22C00321099\\x22],\\x22demo\\x22:[\\x22BBMVersion/2.2\\x22,\\x22DeviceType/GT-I9500\\x22,\\x22OSVersion/Android 4.4 KitKat\\x22,\\x22Manufacturer/SAMSUNG\\x22,\\x22Platform/Android\\x22,\\x22DOB/1991\\x22,\\x22Country/ID\\x22,\\x22realage/22\\x22,\\x22Age/18-24\\x22],\\x22device\\x22:{\\x22platform\\x22:\\x22Android\\x22,\\x22os\\x22:\\x224.4.2.0\\x22,\\x22time\\x22:\\x22Thu, 12 Jun 2014 10:38:56 +0700\\x22,\\x22mcc\\x22:\\x22510\\x22,\\x22device\\x22:\\x22GT-I9500\\x22,\\x22sd\\x22:\\x223\\x22,\\x22carrier\\x22:\\x22Indosat\\x22,\\x22mnc\\x22:\\x221\\x22,\\x22mf\\x22:\\x22samsung\\x22,\\x22sh\\x22:\\x22640\\x22,\\x22sw\\x22:\\x22360\\x22},\\x22tastes\\x22:[\\x22Technology & Computing/Internet Technology\\x22]}\" \"1\" \"Keep-Alive\" \"bbmcore\" \"114.4.23.89, 192.168.1.89\"";
        AccessMetric metric = metricParser.parse(s);

        assertNotNull(metric);

        ConcurrentHashMap<String , String> metricFormatted = metric.format();

        assertEquals(8, metricFormatted.size());
        assertEquals(1402544220L, Long.parseLong(metricFormatted.get("timestamp")));
        assertEquals(1L, Long.parseLong(metricFormatted.get("requests")));
        assertEquals(15L, Long.parseLong(metricFormatted.get("size")));
        assertEquals(0, Double.parseDouble(metricFormatted.get("request_time")), 0.0001);
        assertEquals(0, Double.parseDouble(metricFormatted.get("upstream_time")), 0.0001);
        assertEquals(1L, Long.parseLong(metricFormatted.get("POST")));
        assertEquals(1L, Long.parseLong(metricFormatted.get("ad")));
        assertEquals(1L, Long.parseLong(metricFormatted.get("204")));

    }

    @Test
    public void testMetricParseLimitedMetrics2() throws Exception {
        AccessMetricParser metricParser = new AccessMetricParser("'$remote_addr - $remote_user [$time_local] '" +
                "           '$status $body_bytes_sent \"$request_body\" '" +
                "           '\"$connection_requests\" \"$http_connection\" '" +
                "           '\"$http_user_agent\" \"$http_x_forwarded_for\"");

        HashMap<String, Integer> logFormat = metricParser.getLogFormat();
        int value = logFormat.get("fields");
        assertEquals(12, value);
        value = logFormat.get("timestamp");
        assertEquals(3, value);
        value = logFormat.get("size");
        assertEquals(6, value);
        value = logFormat.get("code");
        assertEquals(5, value);

        String s = "68.171.238.156 - - [12/Jun/2014:03:37:02 +0000] 204 15 \"{\\x22app\\x22:\\x22bbm\\x22,\\x22appver\\x22:\\x222.2.0.28\\x22,\\x22blocked\\x22:[],\\x22channels\\x22:[\\x22C00321099\\x22],\\x22demo\\x22:[\\x22BBMVersion/2.2\\x22,\\x22DeviceType/GT-I9500\\x22,\\x22OSVersion/Android 4.4 KitKat\\x22,\\x22Manufacturer/SAMSUNG\\x22,\\x22Platform/Android\\x22,\\x22DOB/1991\\x22,\\x22Country/ID\\x22,\\x22realage/22\\x22,\\x22Age/18-24\\x22],\\x22device\\x22:{\\x22platform\\x22:\\x22Android\\x22,\\x22os\\x22:\\x224.4.2.0\\x22,\\x22time\\x22:\\x22Thu, 12 Jun 2014 10:38:56 +0700\\x22,\\x22mcc\\x22:\\x22510\\x22,\\x22device\\x22:\\x22GT-I9500\\x22,\\x22sd\\x22:\\x223\\x22,\\x22carrier\\x22:\\x22Indosat\\x22,\\x22mnc\\x22:\\x221\\x22,\\x22mf\\x22:\\x22samsung\\x22,\\x22sh\\x22:\\x22640\\x22,\\x22sw\\x22:\\x22360\\x22},\\x22tastes\\x22:[\\x22Technology & Computing/Internet Technology\\x22]}\" \"1\" \"Keep-Alive\" \"bbmcore\" \"114.4.23.89, 192.168.1.89\"";
        AccessMetric metric = metricParser.parse(s);

        assertNotNull(metric);

        ConcurrentHashMap<String , String> metricFormatted = metric.format();

        assertEquals(6, metricFormatted.size());
        assertEquals(1402544220L, Long.parseLong(metricFormatted.get("timestamp")));
        assertEquals(1L, Long.parseLong(metricFormatted.get("requests")));
        assertEquals(15L, Long.parseLong(metricFormatted.get("size")));
        assertEquals(0, Double.parseDouble(metricFormatted.get("request_time")), 0.0001);
        assertEquals(0, Double.parseDouble(metricFormatted.get("upstream_time")), 0.0001);
        assertEquals(1L, Long.parseLong(metricFormatted.get("204")));
    }
}