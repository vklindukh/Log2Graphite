package com.company.log2graphite.utils;

import org.apache.log4j.Logger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessMetricParser {
    private static final Logger LOG = Logger.getLogger(AccessMetricParser.class);

    public static final String LOG_FINISHED = "__FINISH__";

    private HashMap<String, Integer> logFormat = new HashMap<>();
    private static final String logEntryPattern = "([^\\s\"]+|(?:[^\\s\"]*\"[^\"]*\"[^\\s\"]*)+)(?:\\s|$)";
    private Pattern logPattern = Pattern.compile(logEntryPattern);
    private Matcher matcher = logPattern.matcher("");
    private DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z");

    public AccessMetricParser(HashMap<String, Integer> logFormat) {
        this.logFormat = logFormat;
    }

    public AccessMetricParser(String s) throws ParseException {
        formatParse(s);
    }

    public AccessMetric parse(String s) throws ParseException {
        AccessMetric metric = new AccessMetric();

        if (s.equals(LOG_FINISHED)) {
            metric.setTimestamp(0);
            return metric;
        }

        final int MAX_MATCHED_FIELDS = 20;
        String[] matchedField = new String[MAX_MATCHED_FIELDS];
        int matchedFieldCounter = 0;

        matcher.reset(s.replaceAll("[\\[\\];]", ""));
        while (matcher.find() && (matchedFieldCounter < MAX_MATCHED_FIELDS)) {
            matchedField[matchedFieldCounter] = matcher.group(1);
            matchedFieldCounter++;
        }

        try {
            if (matchedFieldCounter == logFormat.get("fields")) {
                if (!logFormat.containsKey("timestamp")) {
                    throw new ParseException("no required field \"timestamp\"", 0);
                }
                metric.setTimestamp(parseTimestamp(matchedField[logFormat.get("timestamp")].replace("\"", "").replace("'", "") + " " + matchedField[logFormat.get("timestamp") + 1].replace("\"", "").replace("'", "")));
                metric.setMin(Short.parseShort(matchedField[logFormat.get("timestamp")].replace("\"", "").replace("'", "").substring(15, 17)));
                if (logFormat.containsKey("size")) {
                    metric.setSize(Integer.parseInt(matchedField[logFormat.get("size")].replace("\"", "").replace("'", "")));
                }
                if (logFormat.containsKey("request_time")) {
                    metric.setRequest_time(Float.parseFloat(matchedField[logFormat.get("request_time")].replace("\"", "").replace("'", "").equals("-") ? "0" : matchedField[logFormat.get("request_time")].replace("\"", "").replace("'", "")));
                }
                if (logFormat.containsKey("upstream_time")) {
                    metric.setUpstream_time(Float.parseFloat(matchedField[logFormat.get("upstream_time")].replace("\"", "").replace("'", "").equals("-") ? "0" : matchedField[logFormat.get("upstream_time")].replace("\"", "").replace("'", "")));
                }
                if (logFormat.containsKey("request")) {
                    metric.getMethods().insert(matchedField[logFormat.get("request")].replace("\"", "").replace("'", ""));
                    metric.getTypes().insert(matchedField[logFormat.get("request")].replace("\"", "").replace("'", ""));
                }
                if (logFormat.containsKey("code")) {
                    metric.getCodes().put(Integer.parseInt(matchedField[logFormat.get("code")].replace("\"", "").replace("'", "")), 1L);
                }
                metric.setRequests(1);
                metric.setLastUpdated();
                return  metric;
            }
        } catch (NumberFormatException | ParseException m) {
            throw new ParseException(m.toString() + " : " + s, 0);
        }

        throw new ParseException("cannot parse '" + s + "'", 0);
    }

    public HashMap<String, Integer> getLogFormat() {
        return logFormat;
    }


    private void formatParse(String s) throws ParseException {
        if ((s == null) || (s.length() == 0) || (s.equals(""))) {
            LOG.fatal("need access log format template");
            throw new IllegalStateException("no access log format template found");
        }

        LOG.info("got access log format : " + s);

        String[] fields = s.replaceAll("['\"\\[\\];]", "").split("\\s+");
        Map<String, Integer> fieldsHash = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            fieldsHash.put(fields[i], i);
        }

        String requiredFields[] = {"$time_local"};
        for (String requiredKey : requiredFields) {
            if (!fieldsHash.containsKey(requiredKey)) {
                throw new ParseException("no required field '" + requiredKey + "' found", 0);
            }
        }

        logFormat.put("fields", fields.length);

        if (fieldsHash.containsKey("$time_local")) {
            logFormat.put("fields", fields.length + 1);
            logFormat.put("timestamp", fieldsHash.get("$time_local"));
        }

        Map<String, String> knownFields = new HashMap<>();
        knownFields.put("$time_local", "timestamp");
        knownFields.put("$body_bytes_sent", "size");
        knownFields.put("$request", "request");
        knownFields.put("$request_time", "request_time");
        knownFields.put("$upstream_response_time", "upstream_time");
        knownFields.put("$status", "code");

        for (String field : knownFields.keySet()) {
            if (fieldsHash.containsKey(field)) {
                if (logFormat.containsKey("timestamp") && fieldsHash.get(field) > logFormat.get("timestamp")) {
                    logFormat.put(knownFields.get(field), fieldsHash.get(field) + 1);
                } else {
                    logFormat.put(knownFields.get(field), fieldsHash.get(field));
                }
            }
        }

        LOG.info("parsed log format : " + logFormat);
    }

    private long parseTimestamp(String s) throws ParseException {
        if (s.length() < 21) {
            throw new ParseException ("wrong string length", 0);
        }
        Date d =  df.parse(s.substring(0, 18) + "00" + s.substring(20));
        return(d.getTime() / 1000);
    }
}
