package com.company;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private final String logEntryPattern = "([^\\s\"]+|(?:[^\\s\"]*\"[^\"]*\"[^\\s\"]*)+)(?:\\s|$)";
    private Pattern logPattern = Pattern.compile(logEntryPattern);
    private Matcher matcher = logPattern.matcher("");

    /*
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
    } */


    public boolean parse(String s) throws ParseException {
        if (s.equals("__FINISH__")) {
            timestamp = 0;
            return true;
        }

        final int MAX_MATCHED_FIELDS = 20;
        String[] matchedField = new String[MAX_MATCHED_FIELDS];
        int matchedFieldCounter = 0;

        matcher.reset(s);

        while (matcher.find() && (matchedFieldCounter < MAX_MATCHED_FIELDS)) {
            matchedFieldCounter++;
            matchedField[matchedFieldCounter] = matcher.group();
        }

        try {
            if (matchedFieldCounter == 16) { // probably known access.log format
                if (!insertTimestamp(matchedField[4] + matchedField[5])) {
                    return false;
                }
                min = Short.parseShort(matchedField[4].substring(16, 18));
                size = Integer.parseInt(matchedField[8].replace(" ", ""));
                request_time = Float.parseFloat(matchedField[14].replace("\"", "").replace(" ", "").equals("-") ? "0" : matchedField[14].replace("\"", ""));
                upstream_time = Float.parseFloat(matchedField[15].replace("\"", "").replace(" ", "").equals("-") ? "0" : matchedField[15].replace("\"", ""));
                methods.insert(matchedField[6]);
                types.insert(matchedField[6]);
                codes.put(Integer.parseInt(matchedField[7].replace(" ", "")), 1L);
                requests = 1;
                return true;
            }
        } catch (NumberFormatException m) {
            throw new ParseException(m.toString(), 0);
        }

        return false;
    }

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

    private boolean insertTimestamp(String s) throws ParseException {
        if (s.length() < 23) {
            return false;
        }
        DateFormat df = new SimpleDateFormat("'['dd/MMM/yyyy:HH:mm:ss z']'");
        Date d =  df.parse(s.substring(0, 19) + "00" + s.substring(21));
        timestamp = d.getTime() / 1000;
        return (timestamp != 0) ? true : false;
    }

    private class HttpMethod extends HashMapUpdater<String> {
        public void insert(String s) {
            String key = "OTHER_METHOD";
            if (s.substring(1,6).equals("POST "))
                key = "POST";
            else if (s.substring(1,5).equals("GET "))
                key = "GET";
            update(key);
        }
    }

    private class adType extends HashMapUpdater<String> {
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

    private class responseCode extends HashMapUpdater<Integer> {

    }

    private class HashMapUpdater<K> extends HashMap<K, Long> {
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
