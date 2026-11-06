package com.jqueue.contracts.job;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Acknowledges a state-changing action on a job. */
public record JobActionResponse(@JsonProperty("id") String id, @JsonProperty("state") String state) {
}
