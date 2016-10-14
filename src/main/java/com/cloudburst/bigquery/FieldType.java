package com.cloudburst.bigquery;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * BigQuery data types
 */
public enum FieldType {
    STRING,
    INTEGER,
    FLOAT,
    BOOLEAN,
    TIMESTAMP,
    RECORD;

    public static FieldType fromClass (Class cls) {
        if ( cls.equals(String.class) ) {
            return STRING;
        }
        else if ( cls.equals(ZonedDateTime.class) ) {
            return TIMESTAMP;
        }
        else if ( cls.equals(Boolean.class) || cls.equals(Boolean.TYPE) ) {
            return BOOLEAN;
        }
        else if ( cls.equals(Integer.class) || cls.equals(Integer.TYPE) ) {
            return INTEGER;
        }
        else if ( cls.equals(Long.class) || cls.equals(Long.TYPE) ) {
            return INTEGER;
        }
        else if ( cls.equals(Float.class) || cls.equals(Float.TYPE) ) {
            return FLOAT;
        }
        else if ( cls.equals(Double.class) || cls.equals(Double.TYPE) ) {
            return FLOAT;
        }
        else if ( cls.isEnum() ) {
            return STRING;
        }
        else if ( Map.class.isAssignableFrom(cls) ) {
            return RECORD;
        }
        else{
            return RECORD;
        }
    }
}
