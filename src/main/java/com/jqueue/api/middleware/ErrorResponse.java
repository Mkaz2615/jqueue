package com.jqueue.api.middleware;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The single error shape every failing endpoint returns.
 *
 * @param code    the stable, machine-readable identifier a client may branch on
 * @param message the human-readable description
 */
public record ErrorResponse(@JsonProperty("code") String code, @JsonProperty("message") String message) {
}
