package com.company;

import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;


public class AccessMetric {
    private long timestamp = 0;
    private short min = 0;
    private long requests = 0;
    private long size = 0;
    private float request_time = 0;
    private float upstream_time = 0;
    private HttpMethod methods = new HttpMethod();
    private adType types = new adType();
    private responseCode codes = new responseCode();


    private HashMap<String, Integer> logFormat;

    public synchronized boolean update(AccessMetric n) {
        if (this.timestamp != n.timestamp) {
            return false;
        }
        this.requests += n.requests;
        this.size += n.size;
        this.request_time += n.request_time;
        this.upstream_time += n.upstream_time;
        this.methods.update(n.methods);
        this.types.update(n.types);
        this.codes.update(n.codes);
        return true;
    }

    public ConcurrentHashMap<String, String> format() {
        ConcurrentHashMap<String, String> metricFormatted = new ConcurrentHashMap<String, String>();

        metricFormatted.put("timestamp", Long.toString(timestamp));
        metricFormatted.put("requests", Long.toString(requests));
        metricFormatted.put("size", Long.toString(size / requests));
        metricFormatted.put("request_time", Float.toString(request_time / requests));
        metricFormatted.put("upstream_time", Float.toString(upstream_time / requests));

        for (String key : methods.keySet())
            metricFormatted.put(key, Long.toString(methods.get(key)));
        for (String key : types.keySet())
            metricFormatted.put(key, Long.toString(types.get(key)));
        for (int key : codes.keySet())
            metricFormatted.put(Integer.toString(key), Long.toString(codes.get(key)));

        return metricFormatted;
    }

    public String toString() {
        Date time=new Date(timestamp*1000);
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

    public short getMin() {
        return min;
    }

    public void setMin(short min) {
        this.min = min;
    }

    public long getRequests() {
        return requests;
    }

    public void setRequests(long requests) {
        this.requests = requests;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public float getRequest_time() {
        return request_time;
    }

    public void setRequest_time(float request_time) {
        this.request_time = request_time;
    }

    public float getUpstream_time() {
        return upstream_time;
    }

    public void setUpstream_time(float upstream_time) {
        this.upstream_time = upstream_time;
    }

    public HttpMethod getMethods() {
        return methods;
    }

    public void setMethods(HttpMethod methods) {
        this.methods = methods;
    }

    public adType getTypes() {
        return types;
    }

    public void setTypes(adType types) {
        this.types = types;
    }

    public responseCode getCodes() {
        return codes;
    }

    public void setCodes(responseCode codes) {
        this.codes = codes;
    }



    class HttpMethod extends HashMapUpdater<String> {
        public void insert(String s) {
            String key = "OTHER_METHOD";
            if (s.substring(1,6).equals("POST "))
                key = "POST";
            else if (s.substring(1,5).equals("GET "))
                key = "GET";
            update(key);
        }
    }

    class adType extends HashMapUpdater<String> {
        public void insert(String s) {
            String key = "type_unknown";
            int position = s.indexOf(" ");
            if (position > 0) {
                if (s.substring(position + 1, position + 14).equals("/adserver/ad?"))
                    key = "ad";
                else if (s.substring(position + 1, position + 17).equals("/adserver/track?"))
                    key = "track";
            }
            update(key);
        }
    }

    class responseCode extends HashMapUpdater<Integer> {

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
}
