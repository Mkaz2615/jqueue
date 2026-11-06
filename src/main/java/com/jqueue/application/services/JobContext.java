package com.jqueue.application.services;

import java.nio.charset.StandardCharsets;

import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;

/**
 * What a handler receives. It exposes the job's identity and payload and nothing else — a
 * handler has no business transitioning a job's state, and the type system says so.
 *
 * @param maxRetries exposed so a handler can behave differently on its last attempt, for
 *                   example by writing to a fallback store instead of failing
 */
public record JobContext(
        JobId id,
        QueueName queue,
        TaskType taskType,
        byte[] payload,
        int attempt,
        int maxRetries) {

    public JobContext {
        payload = payload == null ? new byte[0] : payload.clone();
    }

    /** Returns a defensive copy of the payload bytes. */
    @Override
    public byte[] payload() {
        return payload.clone();
    }

    /** The payload decoded as UTF-8 text. */
    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
