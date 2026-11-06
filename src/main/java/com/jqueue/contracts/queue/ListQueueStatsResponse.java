package com.jqueue.contracts.queue;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The dashboard's top-level view: every queue plus the rolled-up totals, so the header does
 * not have to be computed client side.
 */
public record ListQueueStatsResponse(
        @JsonProperty("queues") List<QueueStatsResponse> queues,
        @JsonProperty("totals") QueueStatsResponse totals) {
}
