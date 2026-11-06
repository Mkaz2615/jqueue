package com.jqueue.tasks;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jqueue.application.services.HandlerRegistry;
import com.jqueue.application.services.JobContext;
import com.jqueue.application.services.JobHandler;
import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.vo.TaskType;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The example handlers this service registers at startup.
 *
 * <p>They exist so the project runs end to end out of the box: start it, enqueue a job, and
 * watch the dashboard and Grafana fill. In a real deployment this is where an application's
 * own handlers live, and adding one is a single {@code register} call — nothing in the
 * queue engine changes.
 */
public final class DemoTasks {

    public static final String EMAIL_SEND = "email.send";
    public static final String REPORT_GENERATE = "report.generate";
    public static final String FLAKY = "demo.flaky";
    public static final String SLOW = "demo.slow";

    private static final Logger log = LoggerFactory.getLogger(DemoTasks.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private DemoTasks() {
    }

    /**
     * Binds every example handler.
     *
     * <p>It fails on the first error rather than continuing, because a registry that is half
     * built would leave the process advertising itself as ready for work it cannot route.
     */
    public static void registerAll(HandlerRegistry registry) {
        Map<String, JobHandler> handlers = new LinkedHashMap<>();
        handlers.put(EMAIL_SEND, new EmailHandler());
        handlers.put(REPORT_GENERATE, new ReportHandler());
        handlers.put(FLAKY, new FlakyHandler(0.5));
        handlers.put(SLOW, new SlowHandler());
        handlers.forEach((type, handler) -> registry.register(TaskType.of(type), handler));
    }

    /** Simulates sending an email. Expects {@code {"to": ..., "subject": ..., "body": ...}}. */
    public static final class EmailHandler implements JobHandler {
        @Override
        public void handle(JobContext job) throws InterruptedException {
            JsonNode payload;
            try {
                payload = JSON.readTree(job.payload());
            } catch (JacksonException e) {
                // A malformed payload will be malformed on every retry, so it is worth
                // distinguishing in the logs from a transient send failure.
                throw AppError.invalid("invalid_payload", "email payload is not valid JSON");
            }
            if (payload == null || !payload.isObject()) {
                throw AppError.invalid("invalid_payload", "email payload is not valid JSON");
            }
            String to = payload.path("to").asString("");
            if (to.isEmpty()) {
                throw AppError.invalid("invalid_payload", "email payload requires a 'to' address");
            }

            Thread.sleep(Duration.ofMillis(80));

            log.atInfo()
                    .addKeyValue("to", to)
                    .addKeyValue("subject", payload.path("subject").asString(""))
                    .log("email sent");
        }
    }

    /** Simulates a CPU-bound report build. */
    public static final class ReportHandler implements JobHandler {
        @Override
        public void handle(JobContext job) throws InterruptedException {
            Thread.sleep(Duration.ofMillis(200 + ThreadLocalRandom.current().nextInt(600)));
            log.info("report generated");
        }
    }

    /**
     * Fails a configurable fraction of the time.
     *
     * <p>It exists to exercise the retry path: with the default policy a flaky job climbs the
     * backoff curve and, if it keeps failing, lands in the dead-letter queue where the
     * dashboard's retry button can pick it up.
     */
    public static final class FlakyHandler implements JobHandler {
        private final double failureRate;

        public FlakyHandler(double failureRate) {
            this.failureRate = failureRate;
        }

        @Override
        public void handle(JobContext job) throws Exception {
            Thread.sleep(Duration.ofMillis(50));
            if (ThreadLocalRandom.current().nextDouble() < failureRate) {
                throw new Exception("simulated downstream failure on attempt " + job.attempt());
            }
            log.info("flaky task succeeded");
        }
    }

    /**
     * Runs longer than a short job timeout, to demonstrate that a job which overruns is
     * cancelled and retried rather than holding its worker slot.
     */
    public static final class SlowHandler implements JobHandler {
        @Override
        public void handle(JobContext job) throws InterruptedException {
            // Thread.sleep honours interruption, which is how the timeout reaches it.
            Thread.sleep(Duration.ofSeconds(5));
            log.info("slow task finished");
        }
    }
}
