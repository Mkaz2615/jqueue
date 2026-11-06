package com.jqueue.infrastructure.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;
import com.jqueue.domain.metrics.Outcome;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Pins the exported metric names, label names and histogram buckets.
 *
 * <p>The provisioned Grafana dashboard and any alert rules query these exact series. A
 * rename here is a silent outage of every panel, so the names are asserted on the scrape
 * output itself rather than on the Micrometer meter names.
 */
class PrometheusMetricsRecorderTest {

    final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    final PrometheusMetricsRecorder recorder = new PrometheusMetricsRecorder(registry);

    static final QueueName QUEUE = QueueName.of("critical");
    static final TaskType TASK = new TaskType("email.send");

    @Test
    void exportsTheFourCoreMetricsUnderTheirDashboardNames() {
        recorder.recordJobProcessed(QUEUE, TASK, Outcome.SUCCESS);
        recorder.recordJobDuration(QUEUE, TASK, Duration.ofMillis(80));
        recorder.recordJobRetry(QUEUE, TASK);
        recorder.recordJobFailed(QUEUE, TASK, "job_timeout");

        String scrape = registry.scrape();

        assertThat(scrape).contains(
                "jqueue_jobs_processed_total{queue=\"critical\",status=\"success\",task_type=\"email.send\"} 1.0",
                "jqueue_job_retries_total{queue=\"critical\",task_type=\"email.send\"} 1.0",
                "jqueue_jobs_failed_total{queue=\"critical\",reason=\"job_timeout\",task_type=\"email.send\"} 1.0",
                "jqueue_job_processing_duration_seconds_count{queue=\"critical\",task_type=\"email.send\"} 1");
    }

    @Test
    void latencyHistogramUsesTheJobScaleBuckets() {
        recorder.recordJobDuration(QUEUE, TASK, Duration.ofMillis(80));

        String scrape = registry.scrape();

        // 5ms to 60s: the usual client defaults top out at 10s and would collapse every slow
        // job into +Inf.
        String series = "jqueue_job_processing_duration_seconds_bucket{queue=\"critical\",task_type=\"email.send\",le=";
        for (String le : new String[] {"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5",
                "1.0", "2.5", "5.0", "10.0", "30.0", "60.0", "+Inf"}) {
            assertThat(scrape).contains(series + "\"" + le + "\"}");
        }
        // An 80ms observation lands above the 50ms bucket and inside the 100ms one.
        assertThat(scrape).contains(series + "\"0.05\"} 0").contains(series + "\"0.1\"} 1");
        // Exactly the configured buckets, plus +Inf: no default percentile buckets mixed in.
        assertThat(scrape.lines().filter(l -> l.startsWith(series)).count()).isEqualTo(14);
    }

    @Test
    void publishesGauges() {
        recorder.setQueueDepth(QUEUE, JobState.PENDING, 7);
        recorder.setQueueDepth(QUEUE, JobState.PENDING, 4);
        recorder.setActiveWorkers(3);

        String scrape = registry.scrape();

        assertThat(scrape)
                .contains("jqueue_queue_depth{queue=\"critical\",state=\"pending\"} 4.0")
                .contains("jqueue_active_workers 3.0");
    }

    @Test
    void anEmptyReasonBecomesUnknownRatherThanAnEmptyLabel() {
        recorder.recordJobFailed(QUEUE, TASK, "");

        assertThat(registry.scrape()).contains("reason=\"unknown\"");
    }
}
