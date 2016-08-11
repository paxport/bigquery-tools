package com.cloudburst.bigquery;


public class SimpleTableIdentifier implements TableIdentifier {
    private String projectId;
    private String datasetId;
    private String tableId;

    public SimpleTableIdentifier(){
    }

    public SimpleTableIdentifier(String projectId,String datasetId, String tableId) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.tableId = tableId;
    }

    public String getProjectId() {
        return projectId;
    }

    public SimpleTableIdentifier setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public SimpleTableIdentifier setDatasetId(String datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    public String getTableId() {
        return tableId;
    }

    public SimpleTableIdentifier setTableId(String tableId) {
        this.tableId = tableId;
        return this;
    }
}
