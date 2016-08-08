package com.cloudburst.bigquery;


import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class CellUtils {

    public static String str(Object cell) {
        if (cell instanceof String) {
            return (String) cell;
        } else {
            return null;
        }
    }

    public static Integer toInteger(Object cell) {
        if (cell instanceof Long) {
            return ((Long) cell).intValue();
        } else if (cell instanceof Integer) {
            return (Integer) cell;
        } else if (cell instanceof String) {
            return Integer.parseInt((String) cell);
        } else {
            return null;
        }
    }

    public static Long toLong(Object cell) {
        if (cell instanceof Long) {
            return ((Long) cell);
        } else if (cell instanceof Integer) {
            return new Long((Integer) cell);
        } else if (cell instanceof String) {
            return Double.valueOf((String)cell).longValue();
        } else {
            return null;
        }
    }

    public static Long toLongTimestamp(Object cell) {
        if (cell instanceof String) {
            Double d = Double.valueOf((String)cell);
            d = new Double(d * 1000d);
            return d.longValue();
        } else {
            return null;
        }
    }

    public static ZonedDateTime toZonedDateTime(Object cell) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(toLong(cell)), ZoneId.systemDefault());
    }

}
