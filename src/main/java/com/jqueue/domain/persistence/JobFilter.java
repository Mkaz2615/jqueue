package com.jqueue.domain.persistence;

import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;

/**
 * Narrows a job listing. A {@code null} criterion matches everything.
 *
 * @param queue    restricts the listing to one queue
 * @param state    restricts the listing to one lifecycle state
 * @param taskType restricts the listing to one handler routing key
 * @param offset   the number of matches to skip
 * @param limit    caps the page size; zero means the repository's default
 */
public record JobFilter(QueueName queue, JobState state, TaskType taskType, long offset, long limit) {

    /** An unfiltered listing. */
    public static JobFilter all(long limit) {
        return new JobFilter(null, null, null, 0, limit);
    }
}
