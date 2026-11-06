package com.jqueue.contracts.queue;

import com.fasterxml.jackson.annotation.JsonProperty;

/** The depth snapshot of one queue. */
public record QueueStatsResponse(
        @JsonProperty("queue") String queue,
        @JsonProperty("paused") boolean paused,
        @JsonProperty("weight") int weight,
        @JsonProperty("pending") long pending,
        @JsonProperty("active") long active,
        @JsonProperty("scheduled") long scheduled,
        @JsonProperty("retrying") long retrying,
        @JsonProperty("completed") long completed,
        @JsonProperty("dead") long dead,
        @JsonProperty("backlog") long backlog,
        @JsonProperty("total") long total) {
}
