package com.jqueue.api;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import com.jqueue.api.handlers.JobController;
import com.jqueue.api.handlers.QueueController;
import com.jqueue.api.handlers.SupportController;
import com.jqueue.api.middleware.GlobalExceptionHandler;
import com.jqueue.api.middleware.PublicPortFilter;
import com.jqueue.api.middleware.RequestIdFilter;
import com.jqueue.application.enqueuer.DefaultEnqueuerService;
import com.jqueue.application.enqueuer.EnqueuerConfig;
import com.jqueue.application.inspector.DefaultInspectorService;
import com.jqueue.application.processor.DefaultHandlerRegistry;
import com.jqueue.domain.job.vo.TaskType;
import com.jqueue.testing.InMemoryStore;
import com.jqueue.testing.MutableClock;
import com.jqueue.testing.SequenceIdGenerator;

/**
 * Pins the HTTP contract: JSON field names, status codes and the error shape.
 *
 * <p>The controllers run against the real services over the in-memory store, so this is a
 * full request-to-storage path with no Redis and no network.
 */
class ApiContractTest {

    static final Instant NOW = Instant.parse("2026-01-15T10:00:00Z");

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        InMemoryStore store = new InMemoryStore();
        MutableClock clock = new MutableClock(NOW);
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();
        registry.register(new TaskType("email.send"), job -> {
        });

        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();

        mvc = MockMvcBuilders
                .standaloneSetup(
                        new JobController(
                                new DefaultEnqueuerService(store, store, store, new SequenceIdGenerator("job"), clock,
                                        new EnqueuerConfig(Duration.ofHours(1))),
                                new DefaultInspectorService(store, store, store, clock)),
                        new QueueController(new DefaultInspectorService(store, store, store, clock)),
                        new SupportController(registry, "1.0.0"))
                .setControllerAdvice(new GlobalExceptionHandler())
                .setValidator(validator)
                .addFilters(new RequestIdFilter(new SequenceIdGenerator("req")), new PublicPortFilter(8080))
                .build();
    }

    @Test
    void enqueueReturns201WithSnakeCaseFields() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content("""
                        {"task_type":"email.send","queue":"critical","priority":"high",
                         "payload":{"to":"a@b.c"},"max_retries":3}"""))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value("job-1"))
                .andExpect(jsonPath("$.task_type").value("email.send"))
                .andExpect(jsonPath("$.state").value("pending"))
                .andExpect(jsonPath("$.process_at").value("2026-01-15T10:00:00Z"))
                .andExpect(jsonPath("$.deduplicated").value(false));

        mvc.perform(get("/api/v1/jobs/job-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.payload.to").value("a@b.c"))
                .andExpect(jsonPath("$.retry_policy.max_retries").value(3))
                .andExpect(jsonPath("$.retry_policy.backoff_strategy").value("exponential"))
                .andExpect(jsonPath("$.attempts", hasSize(0)))
                // omitempty fields are absent rather than null or empty.
                .andExpect(jsonPath("$.completed_at").doesNotExist())
                .andExpect(jsonPath("$.last_error").doesNotExist());
    }

    @Test
    void aRepeatedIdempotencyKeyReturns200WithTheOriginalJob() throws Exception {
        String body = "{\"task_type\":\"email.send\",\"idempotency_key\":\"welcome-42\"}";
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("job-1"))
                .andExpect(jsonPath("$.deduplicated").value(true));
    }

    @Test
    void validationErrorsUseTheStandardShapeAndJsonFieldNames() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"max_retries\":500,\"priority\":\"urgent\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_request"))
                .andExpect(jsonPath("$.message", containsString("task_type is required")))
                .andExpect(jsonPath("$.message", containsString("max_retries must be at most 100")))
                .andExpect(jsonPath("$.message", containsString("priority must be one of")));
    }

    @Test
    void malformedJsonIsABadRequest() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content("{oops"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_request"));
    }

    @Test
    void domainErrorsMapToStatusCodes() throws Exception {
        mvc.perform(get("/api/v1/jobs/nope"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("job_not_found"));

        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"task_type\":\"x\",\"delay_seconds\":5,\"run_at\":\"2030-01-01T00:00:00Z\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_schedule_time"));

        mvc.perform(patch("/api/v1/queues/ghost/pause").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"paused\":true}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("queue_not_found"));
    }

    @Test
    void listQueryParametersAreValidated() throws Exception {
        mvc.perform(get("/api/v1/jobs").param("state", "zombie"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_request"));
        mvc.perform(get("/api/v1/jobs").param("limit", "501"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/jobs").param("limit", "abc"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/jobs").param("task_type", "email.send"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobs", hasSize(0)))
                .andExpect(jsonPath("$.limit").value(50));
    }

    @Test
    void killThenDeleteAJob() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"task_type\":\"email.send\",\"delay_seconds\":600}"))
                .andExpect(jsonPath("$.state").value("scheduled"));

        mvc.perform(post("/api/v1/jobs/job-1/kill"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("dead"));
        mvc.perform(delete("/api/v1/jobs/job-1")).andExpect(status().isNoContent());
        mvc.perform(get("/api/v1/jobs/job-1")).andExpect(status().isNotFound());
    }

    @Test
    void healthAndReadinessAndRequestIdHeader() throws Exception {
        mvc.perform(get("/health").header(RequestIdFilter.HEADER, "abc-123"))
                .andExpect(status().isOk())
                .andExpect(header().string(RequestIdFilter.HEADER, "abc-123"))
                .andExpect(jsonPath("$.status").value("ok"))
                .andExpect(jsonPath("$.version").value("1.0.0"));

        mvc.perform(get("/ready"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.registered_tasks[0]").value("email.send"));
    }

    @Test
    void readinessIsUnavailableWithNoHandlers() throws Exception {
        MockMvc bare = MockMvcBuilders
                .standaloneSetup(new SupportController(new DefaultHandlerRegistry(), "1.0.0"))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();

        bare.perform(get("/ready"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("no_handlers_registered"));
    }

    @Test
    void thePublicPortHidesPrivateRoutes() throws Exception {
        // MockMvc requests arrive on port 80; a filter keyed to port 80 treats them as public.
        MockMvc publicOnly = MockMvcBuilders
                .standaloneSetup(new SupportController(new DefaultHandlerRegistry(), "1.0.0"))
                .addFilters(new PublicPortFilter(80))
                .build();

        publicOnly.perform(get("/health")).andExpect(status().isOk());
        publicOnly.perform(get("/ready"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("not_found"));
    }
}
