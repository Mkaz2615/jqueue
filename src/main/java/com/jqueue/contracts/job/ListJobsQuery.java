package com.jqueue.contracts.job;

/**
 * The query-string filter for a job listing.
 *
 * @param queue    restricts the listing to one queue
 * @param state    one of scheduled, pending, active, retrying, completed, dead
 * @param taskType restricts the listing to one task type
 * @param offset   the number of matches to skip
 * @param limit    the page size, 1..500; zero means the default of 50
 */
public record ListJobsQuery(String queue, String state, String taskType, long offset, long limit) {

    /** An unfiltered first page. */
    public static ListJobsQuery all() {
        return new ListJobsQuery(null, null, null, 0, 0);
    }
}
