package com.cloudburst.bigquery;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;


//@Ignore("requires live big query access")
public class EmbeddedTest {

    private static BigQueryFactory factory = new BigQueryFactory();
    private static EmbeddedTable table = new EmbeddedTable();

    @BeforeClass
    public static void setup() {
        table.setBigquery(factory.getBigquery());
        if ( table.exists() ) {
            table.delete();
        }
        table.ensureExists();
    }


    @Test
    public void testStreaming () throws IOException {

        ExampleEmbedded embedded = new ExampleEmbedded()
                .setDateTime(ZonedDateTime.now())
                .setText("some text");

        ExampleContainer container = new ExampleContainer()
                .setEmbedded(embedded)
                .setRequiredString("required string");

        TableDataInsertAllResponse res = table.insertItem(container);

    }


}
