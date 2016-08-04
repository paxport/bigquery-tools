package com.cloudburst.bigquery;

import com.google.api.services.bigquery.model.TableRow;

public class BigQueryUtils {


    public static void waitFor(Condition condition) {
        waitFor(condition, 60000);
    }

    public static void waitFor(Condition condition, long timeoutMillis) {
        long toa = System.currentTimeMillis() + timeoutMillis;
        while (!condition.eval()) {
            if (System.currentTimeMillis() > toa) {
                throw new RuntimeException("Timed out waiting for condition to become true");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }


    /**
     * Memcache has key size limit of 250 chars so we need to hash the sql to arrive at a cache key.
     * 64bits is safer then 32 default.
     */
    public static Long cacheKey(String... sql) {
        long hash = 1125899906842597L; // prime
        for (String s : sql) {
            int len = s.length();
            for (int i = 0; i < len; i++) {
                hash = 31 * hash + s.charAt(i);
            }
        }
        return hash;
    }

    public static String str(TableRow row, int idx) {
        Object obj = row.getF().get(idx).getV();
        if (obj instanceof String) {
            return (String) obj;
        } else {
            return null;
        }
    }

    public static Long ts(TableRow row, int idx) {
        Object obj = row.getF().get(idx).getV();
        if (obj instanceof Long) {
            return (Long) obj;
        } else if (obj instanceof String) {
            return Long.parseLong((String) obj);
        } else {
            return null;
        }
    }

    public static Integer integer(TableRow row, int idx) {
        Object obj = row.getF().get(idx).getV();
        if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        } else {
            return null;
        }
    }

    public static Double toUnixTimestamp(long ts) {
        return new Double(ts / 1000l);
    }

    public interface Condition {
        boolean eval();
    }

}
