package com.jqueue.application.services;

import com.jqueue.contracts.job.BulkActionResponse;
import com.jqueue.contracts.job.JobActionResponse;
import com.jqueue.contracts.job.JobResponse;
import com.jqueue.contracts.job.ListJobsQuery;
import com.jqueue.contracts.job.ListJobsResponse;
import com.jqueue.contracts.queue.ListQueueStatsResponse;
import com.jqueue.contracts.queue.QueueActionResponse;

/**
 * The read-and-operate side of the API: everything the dashboard does once a job exists.
 *
 * <p>It is separate from {@link EnqueuerService} because the two have genuinely different
 * consumers. A producer that only submits work should not be handed a dependency that can
 * also delete jobs and pause queues.
 */
public interface InspectorService {

    /** Returns one job in full. */
    JobResponse getJob(String id);

    /** Returns a filtered, paginated page of jobs. */
    ListJobsResponse listJobs(ListJobsQuery query);

    /** Returns a dead job to its queue with a fresh retry budget. */
    JobActionResponse retryJob(String id);

    /** Moves a job to the dead-letter queue, skipping remaining retries. */
    JobActionResponse killJob(String id);

    /** Removes a job entirely. */
    void deleteJob(String id);

    /** Requeues every dead job in a queue and reports how many moved. */
    BulkActionResponse retryAllDead(String queue);

    /** Returns the depth snapshot of every queue. */
    ListQueueStatsResponse listQueueStats();

    /** Suspends or resumes consumption of a queue. */
    QueueActionResponse setQueuePaused(String queue, boolean paused);
}
