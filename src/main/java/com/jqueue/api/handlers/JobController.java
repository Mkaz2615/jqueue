package com.jqueue.api.handlers;

import java.util.Set;

import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.jqueue.application.services.EnqueuerService;
import com.jqueue.application.services.InspectorService;
import com.jqueue.common.errors.AppError;
import com.jqueue.contracts.job.EnqueueJobRequest;
import com.jqueue.contracts.job.EnqueueJobResponse;
import com.jqueue.contracts.job.JobActionResponse;
import com.jqueue.contracts.job.JobResponse;
import com.jqueue.contracts.job.ListJobsQuery;
import com.jqueue.contracts.job.ListJobsResponse;

/**
 * The job resource.
 *
 * <pre>
 * POST   /api/v1/jobs                 enqueue a job, immediately or scheduled
 * GET    /api/v1/jobs                 list jobs, filtered and paginated
 * GET    /api/v1/jobs/{job_id}        fetch one job with its full attempt trail
 * POST   /api/v1/jobs/{job_id}/retry  requeue a dead job with a fresh budget
 * POST   /api/v1/jobs/{job_id}/kill   move a job to the dead-letter queue
 * DELETE /api/v1/jobs/{job_id}        remove a job entirely
 * </pre>
 *
 * <p>Retry and kill are POSTs on a sub-path rather than a PATCH on the job, because they are
 * commands with side effects beyond the job document — they move it between queues — and
 * modelling them as a field update would hide that.
 *
 * <p>The controller depends on the two service interfaces separately rather than on one
 * combined facade, so its dependency list states exactly what it can do.
 */
@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private static final Set<String> STATES =
            Set.of("scheduled", "pending", "active", "retrying", "completed", "dead");

    private final EnqueuerService enqueuer;
    private final InspectorService inspector;

    public JobController(EnqueuerService enqueuer, InspectorService inspector) {
        this.enqueuer = enqueuer;
        this.inspector = inspector;
    }

    /**
     * Enqueues a job.
     *
     * <ul>
     *   <li>201 — the job was created</li>
     *   <li>200 — an idempotency key matched an existing job, which is returned</li>
     *   <li>400 — the request violated a domain invariant</li>
     *   <li>409 — the idempotency key is held by a job that no longer exists</li>
     * </ul>
     */
    @PostMapping
    public ResponseEntity<EnqueueJobResponse> enqueue(@Valid @RequestBody EnqueueJobRequest request) {
        EnqueueJobResponse response = enqueuer.enqueue(request);
        // A deduplicated enqueue created nothing, so 200 is the honest answer; 201 would tell
        // the client a resource came into existence on this call.
        HttpStatus status = response.deduplicated() ? HttpStatus.OK : HttpStatus.CREATED;
        return ResponseEntity.status(status).body(response);
    }

    @GetMapping
    public ListJobsResponse list(
            @RequestParam(name = "queue", required = false) String queue,
            @RequestParam(name = "state", required = false) String state,
            @RequestParam(name = "task_type", required = false) String taskType,
            @RequestParam(name = "offset", required = false, defaultValue = "0") long offset,
            @RequestParam(name = "limit", required = false, defaultValue = "0") long limit) {
        if (state != null && !state.isEmpty() && !STATES.contains(state)) {
            throw AppError.invalid("invalid_request",
                    "state must be one of scheduled, pending, active, retrying, completed, dead");
        }
        if (offset < 0) {
            throw AppError.invalid("invalid_request", "offset must be at least 0");
        }
        if (limit < 0 || limit > 500) {
            throw AppError.invalid("invalid_request", "limit must be between 1 and 500");
        }
        return inspector.listJobs(new ListJobsQuery(queue, state, taskType, offset, limit));
    }

    @GetMapping("/{job_id}")
    public JobResponse get(@PathVariable("job_id") String jobId) {
        return inspector.getJob(jobId);
    }

    @PostMapping("/{job_id}/retry")
    public JobActionResponse retry(@PathVariable("job_id") String jobId) {
        return inspector.retryJob(jobId);
    }

    @PostMapping("/{job_id}/kill")
    public JobActionResponse kill(@PathVariable("job_id") String jobId) {
        return inspector.killJob(jobId);
    }

    @DeleteMapping("/{job_id}")
    public ResponseEntity<Void> delete(@PathVariable("job_id") String jobId) {
        inspector.deleteJob(jobId);
        return ResponseEntity.noContent().build();
    }
}
