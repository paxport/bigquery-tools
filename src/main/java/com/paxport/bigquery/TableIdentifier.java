package com.paxport.bigquery;

public interface TableIdentifier {

    String getProjectId();

    String getDatasetId();

    String getTableId();
}
