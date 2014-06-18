package com.company;

import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class AccessMetricHashMapTest {

    private AccessMetricParser metricParser = new AccessMetricParser();

    @org.junit.Test
    public void multiplyMetrics() throws Exception {
        AccessMetricHashMap h = new AccessMetricHashMap();


        String s = "68.171.238.156 - - [12/Jun/2014:03:37:02 +0000] \"POST /adserver/ad?uid=0OiOYJKNQ4dIKS_15AxmApjZ6L9_jI3685vOmHxmviw.&test=0&type=SPAU HTTP/1.1\" 204 15 \"{\\x22app\\x22:\\x22bbm\\x22,\\x22appver\\x22:\\x222.2.0.28\\x22,\\x22blocked\\x22:[],\\x22channels\\x22:[\\x22C00321099\\x22],\\x22demo\\x22:[\\x22BBMVersion/2.2\\x22,\\x22DeviceType/GT-I9500\\x22,\\x22OSVersion/Android 4.4 KitKat\\x22,\\x22Manufacturer/SAMSUNG\\x22,\\x22Platform/Android\\x22,\\x22DOB/1991\\x22,\\x22Country/ID\\x22,\\x22realage/22\\x22,\\x22Age/18-24\\x22],\\x22device\\x22:{\\x22platform\\x22:\\x22Android\\x22,\\x22os\\x22:\\x224.4.2.0\\x22,\\x22time\\x22:\\x22Thu, 12 Jun 2014 10:38:56 +0700\\x22,\\x22mcc\\x22:\\x22510\\x22,\\x22device\\x22:\\x22GT-I9500\\x22,\\x22sd\\x22:\\x223\\x22,\\x22carrier\\x22:\\x22Indosat\\x22,\\x22mnc\\x22:\\x221\\x22,\\x22mf\\x22:\\x22samsung\\x22,\\x22sh\\x22:\\x22640\\x22,\\x22sw\\x22:\\x22360\\x22},\\x22tastes\\x22:[\\x22Technology & Computing/Internet Technology\\x22]}\" \"1\" \"Keep-Alive\" \"bbmcore\" \"114.4.23.89, 192.168.1.89\" \"0.002\" \"0.002\" \".\"";
        AccessMetric m = metricParser.parse(s);
        assertNotNull(m);
        h.update(m);

        s = "68.171.238.156 - - [12/Jun/2014:03:37:02 +0000] \"POST /adserver/ad?uid=0OiOYJKNQ4dIKS_15AxmApjZ6L9_jI3685vOmHxmviw.&test=0&type=SPAU HTTP/1.1\" 204 15 \"{\\x22app\\x22:\\x22bbm\\x22,\\x22appver\\x22:\\x222.2.0.28\\x22,\\x22blocked\\x22:[],\\x22channels\\x22:[\\x22C00321099\\x22],\\x22demo\\x22:[\\x22BBMVersion/2.2\\x22,\\x22DeviceType/GT-I9500\\x22,\\x22OSVersion/Android 4.4 KitKat\\x22,\\x22Manufacturer/SAMSUNG\\x22,\\x22Platform/Android\\x22,\\x22DOB/1991\\x22,\\x22Country/ID\\x22,\\x22realage/22\\x22,\\x22Age/18-24\\x22],\\x22device\\x22:{\\x22platform\\x22:\\x22Android\\x22,\\x22os\\x22:\\x224.4.2.0\\x22,\\x22time\\x22:\\x22Thu, 12 Jun 2014 10:38:56 +0700\\x22,\\x22mcc\\x22:\\x22510\\x22,\\x22device\\x22:\\x22GT-I9500\\x22,\\x22sd\\x22:\\x223\\x22,\\x22carrier\\x22:\\x22Indosat\\x22,\\x22mnc\\x22:\\x221\\x22,\\x22mf\\x22:\\x22samsung\\x22,\\x22sh\\x22:\\x22640\\x22,\\x22sw\\x22:\\x22360\\x22},\\x22tastes\\x22:[\\x22Technology & Computing/Internet Technology\\x22]}\" \"1\" \"Keep-Alive\" \"bbmcore\" \"114.4.23.89, 192.168.1.89\" \"0.002\" \"0.002\" \".\"";
        AccessMetric m2 = metricParser.parse(s);
        assertNotNull(m2);
        h.update(m2);

        s = "68.171.238.156 - - [12/Jun/2014:03:38:02 +0000] \"POST /adserver/ad?uid=0OiOYJKNQ4dIKS_15AxmApjZ6L9_jI3685vOmHxmviw.&test=0&type=SPAU HTTP/1.1\" 204 15 \"{\\x22app\\x22:\\x22bbm\\x22,\\x22appver\\x22:\\x222.2.0.28\\x22,\\x22blocked\\x22:[],\\x22channels\\x22:[\\x22C00321099\\x22],\\x22demo\\x22:[\\x22BBMVersion/2.2\\x22,\\x22DeviceType/GT-I9500\\x22,\\x22OSVersion/Android 4.4 KitKat\\x22,\\x22Manufacturer/SAMSUNG\\x22,\\x22Platform/Android\\x22,\\x22DOB/1991\\x22,\\x22Country/ID\\x22,\\x22realage/22\\x22,\\x22Age/18-24\\x22],\\x22device\\x22:{\\x22platform\\x22:\\x22Android\\x22,\\x22os\\x22:\\x224.4.2.0\\x22,\\x22time\\x22:\\x22Thu, 12 Jun 2014 10:38:56 +0700\\x22,\\x22mcc\\x22:\\x22510\\x22,\\x22device\\x22:\\x22GT-I9500\\x22,\\x22sd\\x22:\\x223\\x22,\\x22carrier\\x22:\\x22Indosat\\x22,\\x22mnc\\x22:\\x221\\x22,\\x22mf\\x22:\\x22samsung\\x22,\\x22sh\\x22:\\x22640\\x22,\\x22sw\\x22:\\x22360\\x22},\\x22tastes\\x22:[\\x22Technology & Computing/Internet Technology\\x22]}\" \"1\" \"Keep-Alive\" \"bbmcore\" \"114.4.23.89, 192.168.1.89\" \"0.002\" \"0.002\" \".\"";
        AccessMetric m3 = metricParser.parse(s);
        assertNotNull(m3);
        h.update(m3);

        ConcurrentHashMap<String , String> metricFormatted = h.get(1402544220L).format();

        assertEquals(8, metricFormatted.size());
        assertEquals(1402544220L, Long.parseLong(metricFormatted.get("timestamp")));
        assertEquals(2L, Long.parseLong(metricFormatted.get("requests")));
        assertEquals(15L, Long.parseLong(metricFormatted.get("size")));
        assertEquals(0.002, Double.parseDouble(metricFormatted.get("request_time")), 0.0001);
        assertEquals(0.002, Double.parseDouble(metricFormatted.get("upstream_time")), 0.0001);
        assertEquals(2L, Long.parseLong(metricFormatted.get("POST")));
        assertEquals(2L, Long.parseLong(metricFormatted.get("ad")));
        assertEquals(2L, Long.parseLong(metricFormatted.get("204")));

        metricFormatted = h.get(1402544280L).format();

        assertEquals(8, metricFormatted.size());
        assertEquals(1402544280L, Long.parseLong(metricFormatted.get("timestamp")));
        assertEquals(1L, Long.parseLong(metricFormatted.get("requests")));
        assertEquals(15L, Long.parseLong(metricFormatted.get("size")));
        assertEquals(0.002, Double.parseDouble(metricFormatted.get("request_time")), 0.0001);
        assertEquals(0.002, Double.parseDouble(metricFormatted.get("upstream_time")), 0.0001);
        assertEquals(1L, Long.parseLong(metricFormatted.get("POST")));
        assertEquals(1L, Long.parseLong(metricFormatted.get("ad")));
        assertEquals(1L, Long.parseLong(metricFormatted.get("204")));

    }
}