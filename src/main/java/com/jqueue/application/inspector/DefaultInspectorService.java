package com.jqueue.application.inspector;

import java.time.Clock;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jqueue.application.services.InspectorService;
import com.jqueue.common.errors.AppError;
import com.jqueue.contracts.job.BulkActionResponse;
import com.jqueue.contracts.job.JobActionResponse;
import com.jqueue.contracts.job.JobResponse;
import com.jqueue.contracts.job.ListJobsQuery;
import com.jqueue.contracts.job.ListJobsResponse;
import com.jqueue.contracts.queue.ListQueueStatsResponse;
import com.jqueue.contracts.queue.QueueActionResponse;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;
import com.jqueue.domain.persistence.JobBroker;
import com.jqueue.domain.persistence.JobFilter;
import com.jqueue.domain.persistence.JobPage;
import com.jqueue.domain.persistence.JobRepository;
import com.jqueue.domain.persistence.QueueRepository;
import com.jqueue.hydrator.JobHydrator;

/** Serves the dashboard and the operator endpoints. */
public final class DefaultInspectorService implements InspectorService {

    private static final Logger log = LoggerFactory.getLogger(DefaultInspectorService.class);

    /**
     * Caps a single "retry all dead" sweep so an operator cannot flood a queue with a
     * hundred thousand jobs in one click.
     */
    static final long MAX_BULK_RETRY = 1000;

    private static final long DEFAULT_PAGE_SIZE = 50;

    private final JobRepository jobRepository;
    private final QueueRepository queueRepository;
    private final JobBroker broker;
    private final Clock clock;

    public DefaultInspectorService(
            JobRepository jobRepository,
            QueueRepository queueRepository,
            JobBroker broker,
            Clock clock) {
        this.jobRepository = jobRepository;
        this.queueRepository = queueRepository;
        this.broker = broker;
        this.clock = clock;
    }

    @Override
    public JobResponse getJob(String rawId) {
        Job job = jobRepository.findById(JobId.of(rawId));
        return JobHydrator.toJobResponse(job);
    }

    @Override
    public ListJobsResponse listJobs(ListJobsQuery query) {
        JobFilter filter = toFilter(query);
        JobPage page = jobRepository.list(filter);
        return new ListJobsResponse(
                JobHydrator.toSummaries(page.jobs()), page.total(), filter.offset(), filter.limit());
    }

    /**
     * Returns a dead job to its queue with a fresh retry budget.
     *
     * <p>The aggregate's {@code requeue} is what rejects a job that is not dead, so this
     * service does not re-check the state: one rule, one place.
     */
    @Override
    public JobActionResponse retryJob(String rawId) {
        Job job = jobRepository.findById(JobId.of(rawId));
        job.requeue(clock.instant());
        broker.requeue(job);

        log.atInfo()
                .addKeyValue("job_id", job.id().value())
                .addKeyValue("queue", job.queue().value())
                .log("job requeued by operator");

        return new JobActionResponse(job.id().value(), job.state().wire());
    }

    @Override
    public JobActionResponse killJob(String rawId) {
        Job job = jobRepository.findById(JobId.of(rawId));
        job.kill("killed by operator", clock.instant());
        broker.kill(job);

        log.atInfo().addKeyValue("job_id", job.id().value()).log("job killed by operator");

        return new JobActionResponse(job.id().value(), job.state().wire());
    }

    @Override
    public void deleteJob(String rawId) {
        JobId id = JobId.of(rawId);
        jobRepository.delete(id);
        log.atInfo().addKeyValue("job_id", id.value()).log("job deleted by operator");
    }

    /**
     * Requeues every dead job in a queue.
     *
     * <p>Failures are counted rather than aborted on: one job whose document expired
     * mid-sweep should not stop the other nine hundred from being retried, and the caller
     * gets an honest count of what actually moved.
     */
    @Override
    public BulkActionResponse retryAllDead(String rawQueue) {
        QueueName queue = QueueName.of(rawQueue);
        JobPage page = jobRepository.list(new JobFilter(queue, JobState.DEAD, null, 0, MAX_BULK_RETRY));

        Instant now = clock.instant();
        long affected = 0;

        for (Job job : page.jobs()) {
            try {
                job.requeue(now);
            } catch (AppError notDead) {
                continue;
            }
            try {
                broker.requeue(job);
            } catch (RuntimeException e) {
                log.atError()
                        .addKeyValue("job_id", job.id().value())
                        .addKeyValue("error", AppError.describe(e))
                        .log("failed to requeue dead job during bulk retry");
                continue;
            }
            affected++;
        }

        log.atInfo()
                .addKeyValue("queue", queue.value())
                .addKeyValue("affected", affected)
                .log("bulk retry completed");

        return new BulkActionResponse(affected);
    }

    @Override
    public ListQueueStatsResponse listQueueStats() {
        return JobHydrator.toListQueueStats(queueRepository.statsAll());
    }

    @Override
    public QueueActionResponse setQueuePaused(String rawQueue, boolean paused) {
        QueueName queue = QueueName.of(rawQueue);
        queueRepository.findByName(queue);
        queueRepository.setPaused(queue, paused);

        log.atInfo()
                .addKeyValue("queue", queue.value())
                .addKeyValue("paused", paused)
                .log("queue pause state changed");

        return new QueueActionResponse(queue.value(), paused);
    }

    /** Translates the query DTO into a domain filter. */
    private static JobFilter toFilter(ListJobsQuery query) {
        long limit = query.limit() <= 0 ? DEFAULT_PAGE_SIZE : query.limit();
        QueueName queue = isBlank(query.queue()) ? null : QueueName.of(query.queue());
        JobState state = isBlank(query.state()) ? null : JobState.fromWire(query.state());
        TaskType taskType = isBlank(query.taskType()) ? null : TaskType.of(query.taskType());
        return new JobFilter(queue, state, taskType, query.offset(), limit);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isEmpty();
    }
}
