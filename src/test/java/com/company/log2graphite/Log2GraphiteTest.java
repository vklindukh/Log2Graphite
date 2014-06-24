package com.company.log2graphite;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.company.log2graphite.utils.*;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Log2GraphiteTest {

    private static BlockingQueue<String> logInputQueue = new ArrayBlockingQueue<>(128);
    private static BlockingQueue<AccessMetric> logInputMetric = new ArrayBlockingQueue<>(128);

    @Test
    public void integrityTest1() throws Exception {

        Reader reader = new Reader("./src/test/resources/access.log.gz", logInputQueue);
        reader.run();

        assertEquals(12, logInputQueue.size());

        Props properties = new Props("./src/test/resources/log4j.properties");
        AccessMetricParser accessMetricParser = new AccessMetricParser(properties.getLogFormat());
        LogParser logParser = new LogParser(logInputQueue, logInputMetric, accessMetricParser.getLogFormat());
        logParser.run();

        assertEquals(1, logInputQueue.size());
        assertEquals(9, logInputMetric.size());

        MetricReceiver graphite = Mockito.spy(new Graphite(null, 2003));
        doReturn(true).when(graphite).sent(Mockito.anyLong(), Mockito.anyMapOf(String.class, String.class));

        Collector collector = new Collector(logInputMetric, 60000, graphite);
        collector.run();

        verify(graphite, times(7)).sent(Mockito.anyLong(), Mockito.anyMapOf(String.class, String.class));
    }
}