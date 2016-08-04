package com.cloudburst.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * Created by ajchesney on 03/08/2016.
 */
public class ExampleTable extends ReflectionBigQueryTable<ExampleItem> {

    protected ExampleTable() {
        super(ExampleItem.class, "paxportcloud", "paxportcloud_audit", "example_item");
    }

    @Override
    protected Map<String, TableFieldSchema> customFields() {
        Map<String, TableFieldSchema> map = super.customFields();

        // define tracking map sub record which will handle Map
        TableFieldSchema tracking = field("tracking",FieldType.RECORD,FieldMode.NULLABLE);
        tracking.setFields(new ArrayList<>());
        tracking.getFields().add(field("principal",FieldType.STRING,FieldMode.NULLABLE));
        tracking.getFields().add(field("requestId",FieldType.STRING,FieldMode.NULLABLE));

        map.put("tracking",tracking);

        return map;
    }

    @Override
    protected Set<String> defaultExcludedProperties() {
        Set<String> props = super.defaultExcludedProperties();
        props.add("optionalString");
        //props.add("dateTime");
        props.add("optionalInteger");
        return props;
    }
}
