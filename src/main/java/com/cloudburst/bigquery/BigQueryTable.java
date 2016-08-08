package com.cloudburst.bigquery;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BigQueryTable {

    private final static Logger logger = LoggerFactory.getLogger(BigQueryTable.class);

    @Autowired
    protected Bigquery bigquery;

    protected final String projectId;

    protected final String datasetId;

    protected final String tableId;

    private List<TableFieldSchema> cachedFields;

    protected BigQueryTable (String projectId, String datasetId, String tableId) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.tableId = tableId;
    }

    /**
     * Connect to BigQuery and create table if not there
     */
    public BigQueryTable ensureExists() {
        if (!exists()) {
            create();
        }
        return this;
    }

    public boolean exists() {
        try {
            Table table = bigquery.tables().get(projectId, datasetId, tableId).execute();
            return true;
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                return false;
            } else {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BigQueryTable create() {
        try {
            Table table = bigquery.tables().insert(projectId, datasetId, tableContent()).execute();
            BigQueryUtils.waitFor(() -> exists());
            logger.info("Successfully created table: " + this);
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BigQueryTable delete() {
        if (exists()) {
            try {
                bigquery.tables().delete(projectId, datasetId, tableId).execute();
                BigQueryUtils.waitFor(() -> !exists());
                logger.info("Successfully deleted table: " + this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return this;
    }

    protected Table tableContent() {
        Table table = new Table();
        TableReference ref = new TableReference();
        ref.setDatasetId(getDatasetId());
        ref.setProjectId(getProjectId());
        ref.setTableId(getTableId());
        table.setTableReference(ref);
        table.setFriendlyName(getTableId());
        table.setSchema(tableSchema());
        return table;
    }

    protected TableSchema tableSchema() {
        TableSchema schema = new TableSchema();
        schema.setFields(tableFields());
        return schema;
    }

    protected TableFieldSchema field(String name, FieldType type, FieldMode mode) {
        TableFieldSchema result = new TableFieldSchema();
        result.setName(name);
        result.setType(type.name());
        if (mode != null) {
            result.setMode(mode.name());
        }
        return result;
    }

    public BigQueryTable setBigquery(Bigquery bigquery) {
        this.bigquery = bigquery;
        return this;
    }

    protected List<TableFieldSchema> tableFields(){
        if ( cachedFields == null ) {
            cachedFields = createTableFields();
        }
        return cachedFields;
    }

    protected abstract List<TableFieldSchema> createTableFields();

    public String getProjectId() {
        return projectId;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getTableId() {
        return tableId;
    }

    public String fqn() {
        return " [" + getDatasetId() + "." + getTableId() + "] ";
    }

    public TableDataInsertAllResponse insertRow(Map<String, Object> rowData) throws IOException {
        return insertRows(Collections.singletonList(rowData));
    }

    public TableDataInsertAllResponse insertRows(List<Map<String, Object>> rows) throws IOException {
        List<TableDataInsertAllRequest.Rows> insertRows = rows.stream()
                .map(r -> new TableDataInsertAllRequest.Rows().setJson(r))
                .collect(Collectors.toList());
        TableDataInsertAllResponse response = bigquery.tabledata().insertAll(
                projectId,
                datasetId,
                tableId,
                new TableDataInsertAllRequest().setRows(insertRows)).execute();

        if (response != null && response.getInsertErrors() != null) {
            for (TableDataInsertAllResponse.InsertErrors err : response.getInsertErrors()) {
                for (ErrorProto ep : err.getErrors()) {
                    logger.warn("Error inserting into " + ep.getLocation() + " of " + getTableId() + " --> " + ep.getDebugInfo());
                }
            }
        }
        return response;
    }



    @Override
    public String toString() {
        return "BigQueryTable{" +
                "projectId='" + projectId + '\'' +
                ", datasetId='" + datasetId + '\'' +
                ", tableId='" + tableId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        BigQueryTable that = (BigQueryTable) object;

        if (!getProjectId().equals(that.getProjectId())) {
            return false;
        }
        if (!getDatasetId().equals(that.getDatasetId())) {
            return false;
        }
        return getTableId().equals(that.getTableId());

    }

    @Override
    public int hashCode() {
        int result = getProjectId().hashCode();
        result = 31 * result + getDatasetId().hashCode();
        result = 31 * result + getTableId().hashCode();
        return result;
    }
}
