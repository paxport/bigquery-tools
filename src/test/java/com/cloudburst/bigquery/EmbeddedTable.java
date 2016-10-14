package com.cloudburst.bigquery;

/**
 * Created by ajchesney on 14/10/2016.
 */
public class EmbeddedTable extends ReflectionBigQueryTable<ExampleContainer> {

    public EmbeddedTable() {
        super(ExampleContainer.class, new SimpleTableIdentifier("paxportcloud", "test", "example_container"));
    }
}
