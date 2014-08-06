package com.company.log2graphite.core;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

public class AccessMetric {
    private long timestamp;
    private short min;
    private long requests;
    private long size;
    private FloatMetric request_time = new FloatMetric();
    private FloatMetric upstream_time = new FloatMetric();
    private HttpMethod methods = new HttpMethod();
    private adType types = new adType();
    private ResponseCode codes = new ResponseCode();
    private long lastUpdated;
    private long lastUploaded;

    public synchronized boolean update(AccessMetric n) {
        this.requests += n.requests;
        this.size += n.size;
        this.request_time.update(n.request_time);
        this.upstream_time.update(n.upstream_time);
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
        metricFormatted.put("request_time", String.format("%.4f", request_time.getSum() / requests));
        metricFormatted.put("request_time_min", String.format("%.4f", request_time.getMin()));
        metricFormatted.put("request_time_max", String.format("%.4f", request_time.getMax()));
        metricFormatted.put("request_time_stdev", String.format("%.4f", request_time.getStDev()));
        metricFormatted.put("request_time_99", String.format("%.4f", request_time.get99()));
        metricFormatted.put("upstream_time", String.format("%.4f", upstream_time.getSum() / requests));
        metricFormatted.put("upstream_time_min", String.format("%.4f", upstream_time.getMin()));
        metricFormatted.put("upstream_time_max", String.format("%.4f", upstream_time.getMax()));
        metricFormatted.put("upstream_time_stdev", String.format("%.4f", upstream_time.getStDev()));
        metricFormatted.put("upstream_time_99", String.format("%.4f", upstream_time.get99()));

        for (String key : methods.keySet()) {
            metricFormatted.put(key, Long.toString(methods.get(key)));
        }
        for (String key : types.keySet()) {
            metricFormatted.put(key, Long.toString(types.get(key)));
        }
        for (int key : codes.keySet()) {
            metricFormatted.put(Integer.toString(key), Long.toString(codes.get(key)));
        }

        return metricFormatted;
    }

    public String toString() {
        Date time = new Date(timestamp * 1000);
        String s = "  timestamp : " + Long.toString(timestamp) + " [ " + time + " ]" + System.getProperty("line.separator");
        s += "  min : " + Short.toString(min) + System.getProperty("line.separator");
        s += "  requests : " + Long.toString(requests) + System.getProperty("line.separator");
        s += "  size : "  + Long.toString(size) + System.getProperty("line.separator");
        s += "  request_time : "  + Float.toString(request_time.getSum()) + System.getProperty("line.separator");
        s += "  request_time_min : "  + Float.toString(request_time.getMin()) + System.getProperty("line.separator");
        s += "  request_time_max : "  + Float.toString(request_time.getMax()) + System.getProperty("line.separator");
        s += "  request_time_stdev : "  + Float.toString(request_time.getStDev()) + System.getProperty("line.separator");
        s += "  request_time_99 : "  + Float.toString(request_time.get99()) + System.getProperty("line.separator");
        s += "  upstream_time : "  + Float.toString(upstream_time.getSum()) + System.getProperty("line.separator");
        s += "  upstream_time_min : "  + Float.toString(upstream_time.getMin()) + System.getProperty("line.separator");
        s += "  upstream_time_max : "  + Float.toString(upstream_time.getMax()) + System.getProperty("line.separator");
        s += "  upstream_time_stdev : "  + Float.toString(upstream_time.getStDev()) + System.getProperty("line.separator");
        s += "  upstream_time_99 : "  + Float.toString(upstream_time.get99()) + System.getProperty("line.separator");
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
        this.request_time.set(request_time);
    }

    public void setUpstream_time(float upstream_time) {
        this.upstream_time.set(upstream_time);
    }

    public HttpMethod getMethods() {
        return methods;
    }

    public adType getTypes() {
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

    class adType extends HashMapUpdater<String> {
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

    class FloatMetric {
        private long counter;
        private ArrayList<Float> metrics = new ArrayList<>();
        private float min;
        private float sum;
        private float max;
        boolean sorted = false;

        void set(float metric) {
            sum = metric;
            min = metric;
            max = metric;
            metrics = new ArrayList<>();
            metrics.add(metric);
            counter = 1;
            sorted = true;
        }

        void update(FloatMetric n) {
            this.sum += n.sum;
            if (n.max > this.max) {
                this.max = n.max;
            }
            if (counter == 0 || (n.min < this.min)) {
                this.min = n.min;
            }
            metrics.addAll(n.metrics);
            counter += n.counter;
            sorted = false;
        }

        float getSum() {
            return sum;
        }

        float getMin() {
            return min;
        }

        float getMax() {
            return max;
        }

        float getAverage() {
            return sum / counter;
        }

        float getStDev() {
            if (counter == 0) {
                return 0;
            }
            float average = getAverage();
            double metricsSum = 0;
            for (float m : metrics) {
                metricsSum += Math.pow(average - m, 2);
            }
            return (float) Math.sqrt(metricsSum / counter);
        }

        float get99() {
            int counter99;
            if (counter == 0) {
                return (float) 0.0;
            } else if (counter < 100) {
                counter99 = metrics.size() - 1;
            } else {
                counter99 = (int) (counter * 0.99);
            }
            if (! sorted) {
                Collections.sort(metrics);
                sorted = true;
            }
            return metrics.get(counter99);
        }
    }

 }
