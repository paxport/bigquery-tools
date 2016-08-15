package com.cloudburst.bigquery;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Optional;


@Ignore("requires live big query access")
public class ReflectionTableTest {

    private static BigQueryFactory factory = new BigQueryFactory();
    private static ExampleTable exampleTable = new ExampleTable();

    @BeforeClass
    public static void setup() {
        exampleTable.setBigquery(factory.getBigquery());
        exampleTable.ensureExists();
    }


    @Test
    public void testStreaming () throws IOException {

        ExampleItem one = new ExampleItem()
                .setDateTime(ZonedDateTime.now())
                .setRequiredString("required string")
                .setOptionalInteger(Optional.of(1234));

        one.getTracking().put("principal", "someone");
        one.getTracking().put("requestId", "requestId-1234");

        TableDataInsertAllResponse res = exampleTable.insertItem(one);



    }


}
