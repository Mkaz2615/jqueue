package com.jqueue.contracts.queue;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Acknowledges a queue-level action. */
public record QueueActionResponse(@JsonProperty("queue") String queue, @JsonProperty("paused") boolean paused) {
}
