package com.company;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class AccessMetricParser {
    public static final String LOG_FINISHED = "__FINISH__";

    private final String logEntryPattern = "([^\\s\"]+|(?:[^\\s\"]*\"[^\"]*\"[^\\s\"]*)+)(?:\\s|$)";
    private Pattern logPattern = Pattern.compile(logEntryPattern);
    private Matcher matcher = logPattern.matcher("");
    private DateFormat df = new SimpleDateFormat("'['dd/MMM/yyyy:HH:mm:ss z']'");

    public AccessMetric parse(String s) throws ParseException {
        AccessMetric metric = new AccessMetric();

        if (s.equals(LOG_FINISHED)) {
            metric.setTimestamp(0);
            return metric;
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
                metric.setTimestamp(parseTimestamp(matchedField[4] + matchedField[5]));

                metric.setMin(Short.parseShort(matchedField[4].substring(16, 18)));
                metric.setSize(Integer.parseInt(matchedField[8].replace(" ", "")));
                metric.setRequest_time(Float.parseFloat(matchedField[14].replace("\"", "").replace(" ", "").equals("-") ? "0" : matchedField[14].replace("\"", "")));
                metric.setUpstream_time(Float.parseFloat(matchedField[15].replace("\"", "").replace(" ", "").equals("-") ? "0" : matchedField[15].replace("\"", "")));
                metric.getMethods().insert(matchedField[6]);
                metric.getTypes().insert(matchedField[6]);
                metric.getCodes().put(Integer.parseInt(matchedField[7].replace(" ", "")), 1L);
                metric.setRequests(1);
                return  metric;
            }
        } catch (NumberFormatException | ParseException m) {
            throw new ParseException(m.toString(), 0);
        }

        throw new ParseException("cannot parse", 0);
    }

    private long parseTimestamp(String s) throws ParseException {
        if (s.length() < 23) {
            throw new ParseException ("wrong string length", 0);
        }
        Date d =  df.parse(s.substring(0, 19) + "00" + s.substring(21));
        return(d.getTime() / 1000);
    }

}
