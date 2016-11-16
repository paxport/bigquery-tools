package com.paxport.bigquery.query;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;

public interface Query<E> {

    /**
     * Generate Query String to run
     */
    String getSQL();

    /**
     * Make sense of the query result
     */
    E buildResult(GetQueryResultsResponse result);

}
