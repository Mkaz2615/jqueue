package com.jqueue.application.inspector;

import static com.jqueue.testing.ErrorAssertions.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.jqueue.contracts.job.BulkActionResponse;
import com.jqueue.contracts.job.JobActionResponse;
import com.jqueue.contracts.job.JobResponse;
import com.jqueue.contracts.job.ListJobsQuery;
import com.jqueue.contracts.job.ListJobsResponse;
import com.jqueue.contracts.queue.ListQueueStatsResponse;
import com.jqueue.contracts.queue.QueueActionResponse;
import com.jqueue.domain.job.BackoffStrategy;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.job.RetryPolicy;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;
import com.jqueue.domain.queue.QueueErrorCodes;
import com.jqueue.testing.InMemoryStore;
import com.jqueue.testing.MutableClock;

class DefaultInspectorServiceTest {

    static final Instant NOW = Instant.parse("2026-01-15T10:00:00Z");

    InMemoryStore store;
    MutableClock clock;
    DefaultInspectorService service;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        clock = new MutableClock(NOW);
        service = new DefaultInspectorService(store, store, store, clock);
    }

    /**
     * Stores a job in the given state, driving it there through real domain transitions
     * rather than constructing the state directly — so the fixture itself proves the
     * lifecycle is reachable.
     */
    Job seed(String id, String queue, String taskType, JobState state) {
        Job job = Job.NewJobParams.builder()
                .id(id)
                .queue(QueueName.of(queue))
                .taskType(new TaskType(taskType))
                .priority(Priority.NORMAL)
                .retryPolicy(RetryPolicy.of(0, Duration.ofSeconds(1), Duration.ofMinutes(1), BackoffStrategy.CONSTANT))
                .timeout(Duration.ofSeconds(1))
                .now(NOW)
                .build();

        switch (state) {
            case PENDING -> {
            }
            case ACTIVE -> job.lease("worker-1", NOW, Duration.ofSeconds(1));
            case COMPLETED -> {
                job.lease("worker-1", NOW, Duration.ofSeconds(1));
                job.complete(NOW);
            }
            case DEAD -> {
                job.lease("worker-1", NOW, Duration.ofSeconds(1));
                job.fail(new RuntimeException("boom"), NOW);
            }
            default -> throw new IllegalArgumentException("unsupported seed state " + state);
        }
        store.save(job);
        return job;
    }

    @Test
    void getJob_returnsTheFullRepresentation() {
        seed("job-1", "default", "email.send", JobState.COMPLETED);

        JobResponse res = service.getJob("job-1");

        assertThat(res.id()).isEqualTo("job-1");
        assertThat(res.state()).isEqualTo("completed");
        assertThat(res.taskType()).isEqualTo("email.send");
        assertThat(res.completedAt()).isNotEmpty();
        // The attempt trail is what an operator actually opens a job to read.
        assertThat(res.attempts()).hasSize(1);
        assertThat(res.attempts().getFirst().number()).isEqualTo(1);
    }

    @Test
    void getJob_reportsAnUnknownId() {
        assertCode(() -> service.getJob("nope"), JobErrorCodes.JOB_NOT_FOUND);
    }

    @Test
    void getJob_rejectsAMalformedId() {
        // A malformed id is a bad request, not a missing resource: the two map to different
        // status codes and a client should be able to tell them apart.
        assertCode(() -> service.getJob("bad id with spaces"), JobErrorCodes.INVALID_JOB_ID);
    }

    @Test
    void listJobs_filtersByStateAndQueue() {
        seed("job-dead-1", "default", "email.send", JobState.DEAD);
        seed("job-dead-2", "critical", "email.send", JobState.DEAD);
        seed("job-pending", "default", "email.send", JobState.PENDING);

        ListJobsResponse byState = service.listJobs(new ListJobsQuery(null, "dead", null, 0, 0));
        assertThat(byState.total()).isEqualTo(2);

        ListJobsResponse byQueue = service.listJobs(new ListJobsQuery("critical", "dead", null, 0, 0));
        assertThat(byQueue.total()).isEqualTo(1);
        assertThat(byQueue.jobs().getFirst().id()).isEqualTo("job-dead-2");
    }

    @Test
    void listJobs_paginates() {
        for (int i = 0; i < 5; i++) {
            seed("job-" + (char) ('a' + i), "default", "email.send", JobState.PENDING);
        }

        ListJobsResponse page = service.listJobs(new ListJobsQuery(null, null, null, 2, 2));

        assertThat(page.jobs()).hasSize(2);
        assertThat(page.total()).isEqualTo(5);
        assertThat(page.offset()).isEqualTo(2);
        assertThat(page.limit()).isEqualTo(2);
    }

    @Test
    void listJobs_rejectsAnUnknownState() {
        assertCode(() -> service.listJobs(new ListJobsQuery(null, "zombie", null, 0, 0)),
                JobErrorCodes.INVALID_JOB_STATE);
    }

    @Test
    void retryJob_requeuesADeadJobWithAFreshBudget() {
        seed("job-dead", "default", "email.send", JobState.DEAD);
        clock.advance(Duration.ofHours(1));

        JobActionResponse res = service.retryJob("job-dead");
        assertThat(res.state()).isEqualTo("pending");

        Job stored = store.findById(new JobId("job-dead"));
        assertThat(stored.state()).isEqualTo(JobState.PENDING);
        assertThat(stored.attemptsMade()).isZero();
    }

    @Test
    void retryJob_refusesAJobThatIsNotDead() {
        seed("job-pending", "default", "email.send", JobState.PENDING);

        // The rule lives on the aggregate; this asserts the service does not second-guess
        // it or quietly succeed.
        assertCode(() -> service.retryJob("job-pending"), JobErrorCodes.JOB_NOT_RETRYABLE);
    }

    @Test
    void killJob_movesAnActiveJobToTheDeadLetterQueue() {
        seed("job-active", "default", "demo.slow", JobState.ACTIVE);

        JobActionResponse res = service.killJob("job-active");
        assertThat(res.state()).isEqualTo("dead");

        Job stored = store.findById(new JobId("job-active"));
        assertThat(stored.state()).isEqualTo(JobState.DEAD);
        assertThat(stored.lastError()).isEqualTo("killed by operator");
    }

    @Test
    void deleteJob_removesTheJob() {
        seed("job-1", "default", "email.send", JobState.PENDING);

        service.deleteJob("job-1");

        assertCode(() -> store.findById(new JobId("job-1")), JobErrorCodes.JOB_NOT_FOUND);
    }

    @Test
    void retryAllDead_requeuesEveryDeadJobInTheQueue() {
        seed("job-dead-1", "default", "email.send", JobState.DEAD);
        seed("job-dead-2", "default", "email.send", JobState.DEAD);
        seed("job-dead-other", "critical", "email.send", JobState.DEAD);
        seed("job-ok", "default", "email.send", JobState.COMPLETED);

        BulkActionResponse res = service.retryAllDead("default");

        // Scoped to the named queue, and only dead jobs.
        assertThat(res.affected()).isEqualTo(2);
        assertThat(store.findById(new JobId("job-dead-other")).state()).isEqualTo(JobState.DEAD);
    }

    @Test
    void listQueueStats_rollsUpTotals() {
        seed("job-1", "default", "email.send", JobState.PENDING);
        seed("job-2", "default", "email.send", JobState.COMPLETED);
        seed("job-3", "critical", "email.send", JobState.DEAD);

        ListQueueStatsResponse res = service.listQueueStats();

        assertThat(res.queues()).hasSize(2);
        assertThat(res.totals().pending()).isEqualTo(1);
        assertThat(res.totals().completed()).isEqualTo(1);
        assertThat(res.totals().dead()).isEqualTo(1);
        assertThat(res.totals().total()).isEqualTo(3);
        // Backlog excludes terminal states: only the pending job is owed work.
        assertThat(res.totals().backlog()).isEqualTo(1);
    }

    @Test
    void setQueuePaused_togglesConsumption() {
        seed("job-1", "default", "email.send", JobState.PENDING);

        QueueActionResponse paused = service.setQueuePaused("default", true);
        assertThat(paused.paused()).isTrue();
        assertThat(store.findByName(QueueName.DEFAULT).isPaused()).isTrue();

        QueueActionResponse resumed = service.setQueuePaused("default", false);
        assertThat(resumed.paused()).isFalse();
    }

    @Test
    void setQueuePaused_reportsAnUnknownQueue() {
        assertCode(() -> service.setQueuePaused("ghost", true), QueueErrorCodes.QUEUE_NOT_FOUND);
    }
}
