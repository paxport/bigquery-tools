package com.cloudburst.bigquery;

public interface TableIdentifier {

    String getProjectId();

    String getDatasetId();

    String getTableId();
}
