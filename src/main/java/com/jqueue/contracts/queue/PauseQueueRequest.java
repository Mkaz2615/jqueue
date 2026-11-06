package com.jqueue.contracts.queue;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Toggles consumption of a queue. A missing field means "resume". */
public record PauseQueueRequest(@JsonProperty("paused") Boolean paused) {

    public boolean isPaused() {
        return Boolean.TRUE.equals(paused);
    }
}
