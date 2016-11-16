package com.paxport.bigquery;

public class ExampleContainer {

    private String requiredString;

    private ExampleEmbedded embedded;

    public String getRequiredString() {
        return requiredString;
    }

    public ExampleContainer setRequiredString(String requiredString) {
        this.requiredString = requiredString;
        return this;
    }

    public ExampleEmbedded getEmbedded() {
        return embedded;
    }

    public ExampleContainer setEmbedded(ExampleEmbedded embedded) {
        this.embedded = embedded;
        return this;
    }

    @Override
    public String toString() {
        return "ExampleContainer{" +
                "requiredString='" + requiredString + '\'' +
                ", embedded=" + embedded +
                '}';
    }
}
