package com.jqueue.api.handlers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.jqueue.application.services.InspectorService;
import com.jqueue.contracts.job.BulkActionResponse;
import com.jqueue.contracts.queue.ListQueueStatsResponse;
import com.jqueue.contracts.queue.PauseQueueRequest;
import com.jqueue.contracts.queue.QueueActionResponse;

/**
 * The queue resource.
 *
 * <pre>
 * GET   /api/v1/queues                           depth snapshot of every queue
 * PATCH /api/v1/queues/{queue_name}/pause        suspend or resume consumption
 * POST  /api/v1/queues/{queue_name}/dead/retry   requeue every dead job
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/queues")
public class QueueController {

    private final InspectorService inspector;

    public QueueController(InspectorService inspector) {
        this.inspector = inspector;
    }

    @GetMapping
    public ListQueueStatsResponse list() {
        return inspector.listQueueStats();
    }

    @PatchMapping("/{queue_name}/pause")
    public QueueActionResponse setPaused(
            @PathVariable("queue_name") String queueName,
            @RequestBody PauseQueueRequest request) {
        return inspector.setQueuePaused(queueName, request.isPaused());
    }

    @PostMapping("/{queue_name}/dead/retry")
    public BulkActionResponse retryAllDead(@PathVariable("queue_name") String queueName) {
        return inspector.retryAllDead(queueName);
    }
}
