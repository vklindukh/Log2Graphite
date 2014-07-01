package com.company.log2graphite.core;
import java.text.ParseException;
import java.util.*;

public class AccessMetric {
    private long timestamp;
    private short min;
    private long requests;
    private long size;
    private float request_time;
    private float upstream_time;
    private HttpMethod methods = new HttpMethod();
    private AdType types = new AdType();
    private ResponseCode codes = new ResponseCode();
    private MetricRange<Integer> requestTimeRange;
    private static int percentRequestTimeMax;
    private long lastUpdated;
    private long lastUploaded;

    public AccessMetric () { }

    public AccessMetric (int p) {
        percentRequestTimeMax = p;
    }

    public synchronized boolean update(AccessMetric n) {
        this.requests += n.requests;
        this.size += n.size;
        this.request_time += n.request_time;
        this.upstream_time += n.upstream_time;
        this.methods.update(n.methods);
        this.types.update(n.types);
        this.codes.update(n.codes);
        lastUpdated = System.currentTimeMillis();
        return true;
    }

    public boolean forceUpdate(AccessMetric n) {
        this.timestamp = n.timestamp;
        this.min = n.min;
        return (update(n));
    }

    public HashMap<String, String> format() {
        HashMap<String, String> metricFormatted = new HashMap<>();

        if (requests == 0) {
            return metricFormatted;
        }

        metricFormatted.put("requests", Long.toString(requests));
        metricFormatted.put("size", Long.toString(size / requests));
        metricFormatted.put("request_time", Float.toString(request_time / requests));
        metricFormatted.put("upstream_time", Float.toString(upstream_time / requests));

        for (String key : methods.keySet()) {
            metricFormatted.put(key, Long.toString(methods.get(key)));
        }
        for (String key : types.keySet()) {
            metricFormatted.put(key, Long.toString(types.get(key)));
        }
        for (int key : codes.keySet()) {
            metricFormatted.put(Integer.toString(key), Long.toString(codes.get(key)));
        }

        if (percentRequestTimeMax != 0) {
            calcRange(metricFormatted);
        }
        return metricFormatted;
    }

    private void calcRange(HashMap<String, String> metricFormatted) {
        metricFormatted.put(percentRequestTimeMax +"requests",
                String.valueOf(requestTimeRange.maxMetric(percentRequestTimeMax)));
    }

    public String toString() {
        Date time = new Date(timestamp * 1000);
        String s = "  timestamp : " + Long.toString(timestamp) + " [ " + time + " ]" + System.getProperty("line.separator");
        s += "  min : " + Short.toString(min) + System.getProperty("line.separator");
        s += "  requests : " + Long.toString(requests) + System.getProperty("line.separator");
        s += "  size : "  + Long.toString(size) + System.getProperty("line.separator");
        s += "  request_time : "  + Float.toString(request_time) + System.getProperty("line.separator");
        s += "  upstream_time : "  + Float.toString(upstream_time) + System.getProperty("line.separator");
        s += "  methods : " + methods.toString() + System.getProperty("line.separator");
        s += "  types : " + types.toString() + System.getProperty("line.separator");
        s += "  codes : " + codes.toString() + System.getProperty("line.separator");
        return s;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setMin(short min) {
        this.min = min;
    }

    public void setRequests(long requests) {
        this.requests = requests;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setRequest_time(float request_time) {
        this.request_time = request_time;
    }

    public void setUpstream_time(float upstream_time) {
        this.upstream_time = upstream_time;
    }

    public HttpMethod getMethods() {
        return methods;
    }

    public AdType getTypes() {
        return types;
    }

    public ResponseCode getCodes() {
        return codes;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated() {
        lastUpdated = System.currentTimeMillis();
    }

    public long getLastUploaded() {
        return lastUploaded;
    }

    public void setLastUploaded() {
        lastUploaded = System.currentTimeMillis();
    }

    class HttpMethod extends HashMapUpdater<String> {
        public void insert(String s) throws ParseException {
            try {
                String key = "OTHER_METHOD";
                if ((s.length() > 5) && s.substring(0, 5).equals("POST ")) {
                    key = "POST";
                } else if ((s.length() > 4) && s.substring(0, 4).equals("GET ")) {
                    key = "GET";
                }
                update(key);
            } catch (StringIndexOutOfBoundsException m) {
                System.out.println(s);
                throw new ParseException("cannot parse method", 0);
            }
        }
    }

    class AdType extends HashMapUpdater<String> {
        public void insert(String s) throws ParseException {
            try {
                String key = "type_unknown";
                int position = s.indexOf(" ");
                if (position > 0) {
                    if (s.length() > (position + 16) && s.substring(position + 1, position + 14).equals("/adserver/ad?")) {
                        key = "ad";
                    } else if (s.length() > (position + 18) && s.substring(position + 1, position + 17).equals("/adserver/track?")) {
                        key = "track";
                    }
                }
                update(key);
            } catch (StringIndexOutOfBoundsException m) {
                System.out.println(s);
                throw new ParseException("cannot parse ad type", 0);
            }
        }
    }

    class ResponseCode extends HashMapUpdater<Integer> {

    }

    class HashMapUpdater<K> extends HashMap<K, Long> {
        void update (K key) {
            if (containsKey(key))
                put(key, get(key) + 1);
            else
                put(key, 1L);
        }

        void update(HashMapUpdater<K> n) {
            for (K key : n.keySet()) {
                if (containsKey(key)) {
                    put(key, get(key) + n.get(key));
                } else
                    put(key, n.get(key));
            }
        }
    }

    private class MetricRange<E extends Comparable> extends ArrayList<E> {

        E maxMetric(int percent) {
            if (size() > 99) {
                List<E> unsortList = this;
                Collections.sort(unsortList);
                return get(size() -1 - percent);
            } else {
                throw new IllegalStateException("too many metrics to calculate");
            }
        }
    }
}
