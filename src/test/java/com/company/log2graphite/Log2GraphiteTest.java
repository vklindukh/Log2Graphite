package com.company.log2graphite;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.company.log2graphite.core.*;
import org.junit.Test;
import org.mockito.Mockito;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Log2GraphiteTest {

    @Test
    public void integrityTest1() throws Exception {
        BlockingQueue<String> logInputQueue = new ArrayBlockingQueue<>(128);
        BlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<>(128);

        Args cli = new Args( new String[]{"-f", "file"});
        Reader reader = new Reader("./src/test/resources/access.log.gz", cli, logInputQueue);
        reader.run();

        assertEquals(12, logInputQueue.size());

        Props properties = new Props("./src/test/resources/log4j.properties");
        AccessMetricParser accessMetricParser = new AccessMetricParser(properties.getLogFormat(), properties.getAllowedRequests());
        LogParser logParser = new LogParser(logInputQueue, logInputMetric, accessMetricParser.getLogFormat(), accessMetricParser.getAllowedRequests());
        logParser.run();

        assertEquals(1, logInputQueue.size());
        assertEquals(9, logInputMetric.size());

        MetricReceiver graphite = Mockito.spy(new Graphite(cli.getHostname(), "1.1.1.1", 2003));
        doReturn(true).when(graphite).sent(Mockito.anyLong(), Mockito.anyMapOf(String.class, String.class));

        Collector collector = new Collector(logInputMetric, 60000, graphite);
        collector.run();

        verify(graphite, times(7)).sent(Mockito.anyLong(), Mockito.anyMapOf(String.class, String.class));
        Map<String, String> finalMetric = new HashMap<>();

        finalMetric.put("requests", "2");
        finalMetric.put("requests_taken", "2");
        finalMetric.put("new_sessions", "2");
        finalMetric.put("size", "0");
        finalMetric.put("request_time", "0.0020");
        finalMetric.put("request_time_min", "0.0020");
        finalMetric.put("request_time_max", "0.0020");
        finalMetric.put("request_time_stdev", "0.0000");
        finalMetric.put("request_time_99", "0.0020");
        finalMetric.put("upstream_time", "0.0020");
        finalMetric.put("upstream_time_min", "0.0020");
        finalMetric.put("upstream_time_max", "0.0020");
        finalMetric.put("upstream_time_stdev", "0.0000");
        finalMetric.put("upstream_time_99", "0.0020");
        finalMetric.put("POST", "2");
        finalMetric.put("ad", "2");
        finalMetric.put("204", "2");
        verify(graphite, times(1)).sent(1402544220L, finalMetric);

        finalMetric = new HashMap<>();
        finalMetric.put("requests", "2");
        finalMetric.put("requests_taken", "2");
        finalMetric.put("new_sessions", "2");
        finalMetric.put("size", "0");
        finalMetric.put("request_time", "0.0020");
        finalMetric.put("request_time_min", "0.0020");
        finalMetric.put("request_time_max", "0.0020");
        finalMetric.put("request_time_stdev", "0.0000");
        finalMetric.put("request_time_99", "0.0020");
        finalMetric.put("upstream_time", "0.0020");
        finalMetric.put("upstream_time_min", "0.0020");
        finalMetric.put("upstream_time_max", "0.0020");
        finalMetric.put("upstream_time_stdev", "0.0000");
        finalMetric.put("upstream_time_99", "0.0020");
        finalMetric.put("POST", "2");
        finalMetric.put("ad", "2");
        finalMetric.put("204", "2");
        verify(graphite, times(1)).sent(1402544280L, finalMetric);

        finalMetric = new HashMap<>();
        finalMetric.put("requests", "3");
        finalMetric.put("requests_taken", "3");
        finalMetric.put("new_sessions", "3");
        finalMetric.put("size", "0");
        finalMetric.put("request_time", "0.0020");
        finalMetric.put("request_time_min", "0.0020");
        finalMetric.put("request_time_max", "0.0020");
        finalMetric.put("request_time_stdev", "0.0000");
        finalMetric.put("request_time_99", "0.0020");
        finalMetric.put("upstream_time", "0.0020");
        finalMetric.put("upstream_time_min", "0.0020");
        finalMetric.put("upstream_time_max", "0.0020");
        finalMetric.put("upstream_time_stdev", "0.0000");
        finalMetric.put("upstream_time_99", "0.0020");
        finalMetric.put("POST", "3");
        finalMetric.put("ad", "3");
        finalMetric.put("204", "3");
        verify(graphite, times(1)).sent(1402544340L, finalMetric);

        finalMetric = new HashMap<>();
        finalMetric.put("requests", "1");
        finalMetric.put("requests_taken", "1");
        finalMetric.put("new_sessions", "1");
        finalMetric.put("size", "0");
        finalMetric.put("request_time", "0.0020");
        finalMetric.put("request_time_min", "0.0020");
        finalMetric.put("request_time_max", "0.0020");
        finalMetric.put("request_time_stdev", "0.0000");
        finalMetric.put("request_time_99", "0.0020");
        finalMetric.put("upstream_time", "0.0020");
        finalMetric.put("upstream_time_min", "0.0020");
        finalMetric.put("upstream_time_max", "0.0020");
        finalMetric.put("upstream_time_stdev", "0.0000");
        finalMetric.put("upstream_time_99", "0.0020");
        finalMetric.put("POST", "1");
        finalMetric.put("ad", "1");
        finalMetric.put("204", "1");
        verify(graphite, times(1)).sent(1402544460L, finalMetric);

        finalMetric = new HashMap<>();
        finalMetric.put("requests", "1");
        finalMetric.put("requests_taken", "1");
        finalMetric.put("new_sessions", "1");
        finalMetric.put("size", "0");
        finalMetric.put("request_time", "0.0020");
        finalMetric.put("request_time_min", "0.0020");
        finalMetric.put("request_time_max", "0.0020");
        finalMetric.put("request_time_stdev", "0.0000");
        finalMetric.put("request_time_99", "0.0020");
        finalMetric.put("upstream_time", "0.0020");
        finalMetric.put("upstream_time_min", "0.0020");
        finalMetric.put("upstream_time_max", "0.0020");
        finalMetric.put("upstream_time_stdev", "0.0000");
        finalMetric.put("upstream_time_99", "0.0020");
        finalMetric.put("POST", "1");
        finalMetric.put("ad", "1");
        finalMetric.put("204", "1");
        verify(graphite, times(1)).sent(1402544400L, finalMetric);

        finalMetric = new HashMap<>();
        finalMetric.put("requests", "1");
        finalMetric.put("requests_taken", "0");
        finalMetric.put("new_sessions", "0");
        finalMetric.put("size", "0");
        finalMetric.put("request_time", "0.0000");
        finalMetric.put("request_time_min", "0.0000");
        finalMetric.put("request_time_max", "0.0000");
        finalMetric.put("request_time_stdev", "0.0000");
        finalMetric.put("request_time_99", "0.0000");
        finalMetric.put("upstream_time", "0.0000");
        finalMetric.put("upstream_time_min", "0.0000");
        finalMetric.put("upstream_time_max", "0.0000");
        finalMetric.put("upstream_time_stdev", "0.0000");
        finalMetric.put("upstream_time_99", "0.0000");
        finalMetric.put("OTHER_METHOD", "1");
        finalMetric.put("type_unknown", "1");
        finalMetric.put("408", "1");
        verify(graphite, times(1)).sent(1402960980L, finalMetric);

        finalMetric = new HashMap<>();
        finalMetric.put("requests", "1");
        finalMetric.put("requests_taken", "0");
        finalMetric.put("new_sessions", "1");
        finalMetric.put("size", "252");
        finalMetric.put("request_time", "0.0000");
        finalMetric.put("request_time_min", "0.0000");
        finalMetric.put("request_time_max", "0.0000");
        finalMetric.put("request_time_stdev", "0.0000");
        finalMetric.put("request_time_99", "0.0000");
        finalMetric.put("upstream_time", "0.0000");
        finalMetric.put("upstream_time_min", "0.0000");
        finalMetric.put("upstream_time_max", "0.0000");
        finalMetric.put("upstream_time_stdev", "0.0000");
        finalMetric.put("upstream_time_99", "0.0000");
        finalMetric.put("GET", "1");
        finalMetric.put("type_unknown", "1");
        finalMetric.put("400", "1");
        verify(graphite, times(1)).sent(1402888500L, finalMetric);
    }

    @Test
    public void integrityTest2() throws Exception {
        BlockingQueue<String> logInputQueue = new ArrayBlockingQueue<>(128);
        BlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<>(128);
        BlockingQueue<String> metrics = new ArrayBlockingQueue<>(128);
        ExecutorService execParser = Executors.newFixedThreadPool(1);
        execParser.execute(new TCPServer(metrics));

        Args cli = new Args( new String[]{"-f", "file"});
        Reader reader = new Reader("./src/test/resources/access.log.gz", cli, logInputQueue);
        reader.run();
        Props properties = new Props("./src/test/resources/log4j.properties");
        AccessMetricParser accessMetricParser = new AccessMetricParser(properties.getLogFormat(), properties.getAllowedRequests());
        LogParser logParser = new LogParser(logInputQueue, logInputMetric, accessMetricParser.getLogFormat(), accessMetricParser.getAllowedRequests());
        logParser.run();
        Collector collector = new Collector(logInputMetric, 60000, new Graphite(cli.getHostname(), "127.0.0.1", 2003));
        collector.run();

        Thread.sleep(1000);

        assertEquals(119, metrics.size());

        String metricBase = "access." + InetAddress.getLocalHost().getHostName() + ".";
        HashSet<String> expectedMetricSet = new HashSet<>(Arrays.asList(
                metricBase + "204 1 1402544400",
                metricBase + "204 1 1402544460",
                metricBase + "204 2 1402544220",
                metricBase + "204 2 1402544280",
                metricBase + "204 3 1402544340",
                metricBase + "400 1 1402888500",
                metricBase + "408 1 1402960980",
                metricBase + "GET 1 1402888500",
                metricBase + "OTHER_METHOD 1 1402960980",
                metricBase + "POST 1 1402544400",
                metricBase + "POST 1 1402544460",
                metricBase + "POST 2 1402544220",
                metricBase + "POST 2 1402544280",
                metricBase + "POST 3 1402544340",
                metricBase + "ad 1 1402544400",
                metricBase + "ad 1 1402544460",
                metricBase + "ad 2 1402544220",
                metricBase + "ad 2 1402544280",
                metricBase + "ad 3 1402544340",
                metricBase + "request_time 0.0000 1402888500",
                metricBase + "request_time 0.0020 1402544220",
                metricBase + "request_time 0.0020 1402544280",
                metricBase + "request_time 0.0020 1402544340",
                metricBase + "request_time 0.0020 1402544400",
                metricBase + "request_time 0.0020 1402544460",
                metricBase + "request_time 0.0000 1402960980",
                metricBase + "request_time_min 0.0000 1402888500",
                metricBase + "request_time_min 0.0020 1402544220",
                metricBase + "request_time_min 0.0020 1402544280",
                metricBase + "request_time_min 0.0020 1402544340",
                metricBase + "request_time_min 0.0020 1402544400",
                metricBase + "request_time_min 0.0020 1402544460",
                metricBase + "request_time_min 0.0000 1402960980",
                metricBase + "request_time_max 0.0000 1402888500",
                metricBase + "request_time_max 0.0020 1402544220",
                metricBase + "request_time_max 0.0020 1402544280",
                metricBase + "request_time_max 0.0020 1402544340",
                metricBase + "request_time_max 0.0020 1402544400",
                metricBase + "request_time_max 0.0020 1402544460",
                metricBase + "request_time_max 0.0000 1402960980",
                metricBase + "request_time_99 0.0000 1402888500",
                metricBase + "request_time_99 0.0020 1402544220",
                metricBase + "request_time_99 0.0020 1402544280",
                metricBase + "request_time_99 0.0020 1402544340",
                metricBase + "request_time_99 0.0020 1402544400",
                metricBase + "request_time_99 0.0020 1402544460",
                metricBase + "request_time_99 0.0000 1402960980",
                metricBase + "request_time_stdev 0.0000 1402888500",
                metricBase + "request_time_stdev 0.0000 1402544220",
                metricBase + "request_time_stdev 0.0000 1402544280",
                metricBase + "request_time_stdev 0.0000 1402544340",
                metricBase + "request_time_stdev 0.0000 1402544400",
                metricBase + "request_time_stdev 0.0000 1402544460",
                metricBase + "request_time_stdev 0.0000 1402960980",
                metricBase + "requests 1 1402544400",
                metricBase + "requests 1 1402544460",
                metricBase + "requests 1 1402888500",
                metricBase + "requests 1 1402960980",
                metricBase + "requests 2 1402544220",
                metricBase + "requests 2 1402544280",
                metricBase + "requests 3 1402544340",
                metricBase + "requests_taken 1 1402544400",
                metricBase + "requests_taken 1 1402544460",
                metricBase + "requests_taken 0 1402888500",
                metricBase + "requests_taken 0 1402960980",
                metricBase + "requests_taken 2 1402544220",
                metricBase + "requests_taken 2 1402544280",
                metricBase + "requests_taken 3 1402544340",
                metricBase + "new_sessions 1 1402544400",
                metricBase + "new_sessions 1 1402544460",
                metricBase + "new_sessions 1 1402888500",
                metricBase + "new_sessions 0 1402960980",
                metricBase + "new_sessions 2 1402544220",
                metricBase + "new_sessions 2 1402544280",
                metricBase + "new_sessions 3 1402544340",
                metricBase + "size 0 1402544220",
                metricBase + "size 0 1402544280",
                metricBase + "size 0 1402544340",
                metricBase + "size 0 1402544400",
                metricBase + "size 0 1402544460",
                metricBase + "size 0 1402960980",
                metricBase + "size 252 1402888500",
                metricBase + "type_unknown 1 1402888500",
                metricBase + "type_unknown 1 1402960980",
                metricBase + "upstream_time 0.0000 1402888500",
                metricBase + "upstream_time 0.0000 1402960980",
                metricBase + "upstream_time 0.0020 1402544220",
                metricBase + "upstream_time 0.0020 1402544280",
                metricBase + "upstream_time 0.0020 1402544340",
                metricBase + "upstream_time 0.0020 1402544400",
                metricBase + "upstream_time 0.0020 1402544460",
                metricBase + "upstream_time_min 0.0000 1402888500",
                metricBase + "upstream_time_min 0.0000 1402960980",
                metricBase + "upstream_time_min 0.0020 1402544220",
                metricBase + "upstream_time_min 0.0020 1402544280",
                metricBase + "upstream_time_min 0.0020 1402544340",
                metricBase + "upstream_time_min 0.0020 1402544400",
                metricBase + "upstream_time_min 0.0020 1402544460",
                metricBase + "upstream_time_max 0.0000 1402888500",
                metricBase + "upstream_time_max 0.0000 1402960980",
                metricBase + "upstream_time_max 0.0020 1402544220",
                metricBase + "upstream_time_max 0.0020 1402544280",
                metricBase + "upstream_time_max 0.0020 1402544340",
                metricBase + "upstream_time_max 0.0020 1402544400",
                metricBase + "upstream_time_max 0.0020 1402544460",
                metricBase + "upstream_time_99 0.0000 1402888500",
                metricBase + "upstream_time_99 0.0000 1402960980",
                metricBase + "upstream_time_99 0.0020 1402544220",
                metricBase + "upstream_time_99 0.0020 1402544280",
                metricBase + "upstream_time_99 0.0020 1402544340",
                metricBase + "upstream_time_99 0.0020 1402544400",
                metricBase + "upstream_time_99 0.0020 1402544460",
                metricBase + "upstream_time_stdev 0.0000 1402888500",
                metricBase + "upstream_time_stdev 0.0000 1402960980",
                metricBase + "upstream_time_stdev 0.0000 1402544220",
                metricBase + "upstream_time_stdev 0.0000 1402544280",
                metricBase + "upstream_time_stdev 0.0000 1402544340",
                metricBase + "upstream_time_stdev 0.0000 1402544400",
                metricBase + "upstream_time_stdev 0.0000 1402544460"));

        HashSet<String> metricSet = new HashSet<>(new ArrayList<>(metrics));

        assertEquals(expectedMetricSet, metricSet);
    }

/*
    @Test
    public void max99Test() throws Exception {
        BlockingQueue<String> logInputQueue = new ArrayBlockingQueue<>(128);
        BlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<>(128);
        BlockingQueue<String> metrics = new ArrayBlockingQueue<>(128);
        ExecutorService execParser = Executors.newFixedThreadPool(1);
        execParser.execute(new TCPServer(metrics));

        Args cli = new Args( new String[]{"-f", "file"});
        Reader reader = new Reader("./src/test/resources/access2.log.gz", cli, logInputQueue);
        reader.run();
        Props properties = new Props("./src/test/resources/log4j.properties");
        AccessMetricParser accessMetricParser = new AccessMetricParser(properties.getLogFormat());
        LogParser logParser = new LogParser(logInputQueue, logInputMetric, accessMetricParser.getLogFormat());
        logParser.run();
        Collector collector = new Collector(logInputMetric, 60000, new Graphite(cli.getHostname(), "127.0.0.1", 2003));
        collector.run();

        Thread.sleep(1000);

        assertEquals(30, metrics.size());

        String metricBase = "access." + InetAddress.getLocalHost().getHostName() + ".";
        HashSet<String> expectedMetricSet = new HashSet<>(Arrays.asList(
                metricBase + "200 53175 1406173200",
                metricBase + "400 109 1406173200",
                metricBase + "500 2 1406173200",
                metricBase + "204 13432 1406173200",
                metricBase + "GET 19218 1406173200",
                metricBase + "POST 47500 1406173200",
                metricBase + "ad 47500 1406173200",
                metricBase + "track 19208 1406173200",
                metricBase + "requests 66718 1406173200",
                metricBase + "size 2193 1406173200",
                metricBase + "request_time 0.0075 1406173200",
                metricBase + "request_time_min 0.0000 1406173200",
                metricBase + "request_time_max 40.3550 1406173200",
                metricBase + "request_time_99 0.0150 1406173200",
                metricBase + "request_time_stdev 0.2035 1406173200",
                metricBase + "upstream_time 0.0023 1406173200",
                metricBase + "upstream_time_min 0.0000 1406173200",
                metricBase + "upstream_time_max 0.1030 1406173200",
                metricBase + "upstream_time_99 0.0050 1406173200",
                metricBase + "upstream_time_stdev 0.0027 1406173200",
                metricBase + "204 1 1406173260",
                metricBase + "POST 1 1406173260",
                metricBase + "ad 1 1406173260",
                metricBase + "request_time 0.0020 1406173260",
                metricBase + "request_time_min 0.0020 1406173260",
                metricBase + "request_time_max 0.0020 1406173260",
                metricBase + "request_time_99 0.0020 1406173260",
                metricBase + "request_time_stdev 0.0000 1406173260",
                metricBase + "requests 1 1406173260",
                metricBase + "size 0 1406173260",
                metricBase + "upstream_time 0.0020 1406173260",
                metricBase + "upstream_time_min 0.0020 1406173260",
                metricBase + "upstream_time_max 0.0020 1406173260",
                metricBase + "upstream_time_99 0.0020 1406173260",
                metricBase + "upstream_time_stdev 0.0000 1406173260"
                ));

        HashSet<String> metricSet = new HashSet<>(new ArrayList<>(metrics));

        assertEquals(expectedMetricSet, metricSet);
    }
*/

    private class TCPServer implements Runnable {
        ServerSocket server;
        BlockingQueue<String> metrics;

        public TCPServer(BlockingQueue<String> metrics) {
            try {
                this.metrics = metrics;
                server = new ServerSocket(2003);
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }

        public void run() {
            while (true) {
                try {
                    Socket socket = server.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    boolean connected = true;
                    while (connected) {
                        String line = in.readLine();
                        if (line == null) {
                            connected = false;
                        } else {
                            metrics.put(line);
                        }
                    }
                    in.close();
                } catch (IOException | InterruptedException e) {
                    System.err.println(e);
                    System.exit(1);
                }
            }
        }
    }
}