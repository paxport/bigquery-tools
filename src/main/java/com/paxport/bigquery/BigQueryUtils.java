package com.paxport.bigquery;

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

    public static Double toUnixTimestamp(long ts) {
        return new Double(ts / 1000l);
    }

    public interface Condition {
        boolean eval();
    }

}
