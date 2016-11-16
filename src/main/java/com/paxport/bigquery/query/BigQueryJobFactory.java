package com.paxport.bigquery.query;

import com.paxport.bigquery.BigQueryFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BigQueryJobFactory {

    @Autowired
    private BigQueryFactory factory;

    public <E> QueryJob<E> startQuery(String projectId, Query<E> query) {
        return new QueryJob<>(query, projectId,factory.getBigquery()).start();
    }

    public BigQueryJobFactory setFactory(BigQueryFactory factory) {
        this.factory = factory;
        return this;
    }
}
