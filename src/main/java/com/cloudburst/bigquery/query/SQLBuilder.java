package com.cloudburst.bigquery.query;

import org.joda.time.DateTime;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class SQLBuilder {

    private StringBuilder sb = new StringBuilder();

    private DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public SQLBuilder append(String str) {
        sb.append(str);
        return this;
    }

    public SQLBuilder newLine() {
        sb.append("\n");
        return this;
    }

    public SQLBuilder quote(String str) {
        sb.append("'");
        sb.append(str);
        sb.append("'");
        return this;
    }

    public SQLBuilder ts(ZonedDateTime dt) {
        sb.append(" PARSE_UTC_USEC(\"");
        sb.append(dt.format(DATE_TIME_FORMATTER));
        sb.append("\") ");
        return this;
    }

    public SQLBuilder msec_to_ts(DateTime dt) {
        sb.append(" MSEC_TO_TIMESTAMP(");
        sb.append(dt.getMillis());
        sb.append(") ");
        return this;
    }

    public SQLBuilder msec_to_ts(long dt) {
        sb.append(" MSEC_TO_TIMESTAMP(");
        sb.append(dt);
        sb.append(") ");
        return this;
    }

    public String toString() {
        return sb.toString();
    }
}
