package com.company;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class AccessMetric {
    public long timestamp = 0;
    public short min = 0;
    public long requests = 0;
    public long size = 0;
    public float request_time = 0;
    public float upstream_time = 0;
    public HttpMethod methods = new HttpMethod();
    public adType types = new adType();
    public responseCode codes = new responseCode();

    public AccessMetric copyOf() {
        AccessMetric n = new AccessMetric();
        n.timestamp = timestamp;
        n.min = min;
        n.requests = requests;
        n.size = size;
        n.request_time = request_time;
        n.upstream_time = upstream_time;
        n.methods = (HttpMethod) methods.clone();
        n.types = (adType) types.clone();
        n.codes = (responseCode) codes.clone();
        return n;
    }

    public void insertTimestamp(String s) throws ParseException {
        DateFormat df = new SimpleDateFormat("'['dd/MMM/yyyy:HH:mm:ss z']'");
        Date d =  df.parse(s.substring(0, 19) + "00" + s.substring(21));
        timestamp = d.getTime() / 1000;
    }

    public ConcurrentHashMap<String, String> format() {
        ConcurrentHashMap<String, String> metricFormatted = new ConcurrentHashMap<String, String>();

        metricFormatted.put("timestamp", Long.toString(timestamp));
        metricFormatted.put("requests", Long.toString(requests));
        metricFormatted.put("size", Float.toString((float)size / requests));
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
        String s = "timestamp : " + Long.toString(timestamp) + System.getProperty("line.separator");
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

    public class HttpMethod extends HashMapUpdater<String> {
        public void insert(String s) {
            String key = "OTHER_METHOD";
            if (s.substring(1,6).equals("POST "))
                key = "POST";
            else if (s.substring(1,5).equals("GET "))
                key = "GET";
            update(key);
        }
    }

    public class adType extends HashMapUpdater<String> {
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

    public class responseCode extends HashMapUpdater<Integer> {

    }

    public class HashMapUpdater<K> extends HashMap<K, Long> {
        public void update (K key) {
            if (containsKey(key))
                put(key, get(key) + 1);
            else
                put(key, 1L);
        }

        public void update(HashMapUpdater<K> n) {
            for (K key : n.keySet()) {
                if (containsKey(key)) {
                    put(key, get(key) + n.get(key));
                } else
                    put(key, n.get(key));
            }
        }
    }
}
