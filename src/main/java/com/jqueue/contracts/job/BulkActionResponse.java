package com.jqueue.contracts.job;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Reports how many jobs an operation affected. */
public record BulkActionResponse(@JsonProperty("affected") long affected) {
}
