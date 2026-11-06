package com.jqueue.contracts.job;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A page of job summaries plus the totals a client needs to render pagination without a
 * second request.
 */
public record ListJobsResponse(
        @JsonProperty("jobs") List<JobSummaryResponse> jobs,
        @JsonProperty("total") long total,
        @JsonProperty("offset") long offset,
        @JsonProperty("limit") long limit) {
}
