package com.paxport.bigquery;

import java.time.ZonedDateTime;

public class ExampleEmbedded {

    private String text;

    @Override
    public String toString() {
        return "ExampleEmbedded{" +
                "text='" + text + '\'' +
                ", dateTime=" + dateTime +
                '}';
    }

    private ZonedDateTime dateTime;

    public String getText() {
        return text;
    }

    public ExampleEmbedded setText(String text) {
        this.text = text;
        return this;
    }

    public ZonedDateTime getDateTime() {
        return dateTime;
    }

    public ExampleEmbedded setDateTime(ZonedDateTime dateTime) {
        this.dateTime = dateTime;
        return this;
    }


}
