package com.paxport.bigquery.query;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class QueryJob<E> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Bigquery bigquery;

    private String projectId;
    private long sleepMillis = 1000;
    private int retries = 3;

    private Query<E> query;
    private E result;
    private JobReference ref;
    private boolean resultFromCache = false;

    public QueryJob(Query<E> query, String projectId, Bigquery bigquery) {
        this.query = query;
        this.projectId = projectId;
        this.bigquery = bigquery;
    }

    public Query<E> getQuery() {
        return this.query;
    }

    public QueryJob<E> start() {
        ref = insertJob(query.getSQL());
        return this;
    }

    public boolean isResultReady() {
        if (result != null) {
            logger.info("result is ready");
            return true;
        }
        Job job = isJobComplete(ref);
        if (job != null) {
            result = buildResult(job);
            return true;
        } else {
            return false;
        }
    }

    public E waitForResult() {
        logger.info("waiting for result");
        while (!isResultReady()) {
            sleep();
        }
        return getResult();
    }

    public E getResult() {
        return result;
    }

    public boolean isResultFromCache() {
        return resultFromCache;
    }

    private E buildResult(Job completedJob) {
        GetQueryResultsResponse queryResult;
        int attempts = retries;
        do {
            try {
                queryResult = bigquery.jobs()
                        .getQueryResults(
                                projectId, completedJob
                                        .getJobReference()
                                        .getJobId()
                        ).execute();
                List<TableRow> rows = queryResult.getRows();
                return query.buildResult(queryResult);
            } catch (IOException e) {
                logger.warn("exception getting result rows from bigquery so retrying", e);
            }
        } while (--attempts >= 0);
        throw new RuntimeException("Failed to get result rows from BigQuery after retrying");
    }

    private JobReference insertJob(String sql) {

        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
        config.setQuery(queryConfig);
        job.setConfiguration(config);
        queryConfig.setQuery(sql);
        JobReference jobId = null;
        int attempts = retries;
        do {
            try {
                Bigquery.Jobs.Insert insert = bigquery.jobs().insert(projectId, job);

                insert.setProjectId(projectId);
                jobId = insert.execute().getJobReference();
                logger.info("sent sql bigquery to " + projectId + " with jobRef " + jobId.getJobId() + ":\n" + sql);
                return jobId;
            } catch (IOException e) {
                logger.warn("failed to insert query job so retrying", e);
            }
        } while (--attempts >= 0);
        throw new RuntimeException("Failed to start query job after retrying 3 times: " + sql);
    }

    private Job isJobComplete(JobReference jobId) {
        try {
            Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
            if (pollJob.getStatus().getState().equals("DONE")) {
                long millisInBiqQuery = pollJob.getStatistics().getEndTime() - pollJob.getStatistics().getStartTime();
                logger.info("SQL Job completed in " + millisInBiqQuery + " bigquery millis : " + jobId.getJobId());
                return pollJob;
            } else {
                logger.info("SQL Job " + jobId.getJobId() + " is not yet complete");
                return null;
            }
        } catch (IOException e) {
            logger.warn("failed to poll query job so assuming still running", e);
            return null;
        }
    }

    private void sleep() {
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            logger.warn("sleep interrupted");
        }
    }
}
