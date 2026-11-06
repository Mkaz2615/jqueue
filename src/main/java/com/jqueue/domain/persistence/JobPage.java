package com.jqueue.domain.persistence;

import java.util.List;

import com.jqueue.domain.job.Job;

/**
 * One page of a job listing plus the total match count, which the dashboard needs to render
 * pagination.
 */
public record JobPage(List<Job> jobs, long total) {

    public JobPage {
        jobs = List.copyOf(jobs);
    }
}
