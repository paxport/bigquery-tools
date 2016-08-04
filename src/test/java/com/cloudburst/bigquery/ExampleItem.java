package com.cloudburst.bigquery;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by ajchesney on 03/08/2016.
 */
public class ExampleItem {

    private String requiredString;
    private Optional<String> optionalString;
    private ZonedDateTime dateTime;
    private Optional<Integer> optionalInteger;

    private Map<String,String> tracking = new HashMap<>();

    public Map<String, String> getTracking() {
        return tracking;
    }

    public ExampleItem setTracking(Map<String, String> tracking) {
        this.tracking = tracking;
        return this;
    }

    public String getRequiredString() {
        return requiredString;
    }

    public ExampleItem setRequiredString(String requiredString) {
        this.requiredString = requiredString;
        return this;
    }

    public Optional<String> getOptionalString() {
        return optionalString;
    }

    public ExampleItem setOptionalString(Optional<String> optionalString) {
        this.optionalString = optionalString;
        return this;
    }

    public ZonedDateTime getDateTime() {
        return dateTime;
    }

    public ExampleItem setDateTime(ZonedDateTime dateTime) {
        this.dateTime = dateTime;
        return this;
    }

    public Optional<Integer> getOptionalInteger() {
        return optionalInteger;
    }

    public ExampleItem setOptionalInteger(Optional<Integer> optionalInteger) {
        this.optionalInteger = optionalInteger;
        return this;
    }
}
