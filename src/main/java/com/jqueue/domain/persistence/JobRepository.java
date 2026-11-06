package com.jqueue.domain.persistence;

import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.QueueName;

/**
 * Persists and retrieves Job aggregates.
 *
 * <p>The ports live in the domain, beside the aggregates they serve, and the concrete Redis
 * types live in the infrastructure layer. Dependencies therefore point inwards.
 *
 * <p>This port is intentionally separate from {@link JobBroker}: it answers "what is the
 * state of job X", the broker answers "give me the next job to run". Splitting them keeps
 * read-only callers such as the dashboard from depending on queueing operations they must
 * never invoke.
 */
public interface JobRepository {

    /** Writes the full job document, creating or overwriting it. */
    void save(Job job);

    /** Returns one job, or throws a NOT_FOUND {@code AppError} when the id is unknown. */
    Job findById(JobId id);

    /** Returns a filtered, paginated page of jobs. */
    JobPage list(JobFilter filter);

    /** Removes a job document and its index entries. */
    void delete(JobId id);

    /**
     * Removes completed jobs whose retention window has elapsed and reports how many were
     * removed.
     */
    long purgeCompleted(QueueName queue, long limit);
}
