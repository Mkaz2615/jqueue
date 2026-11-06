package com.jqueue.domain.metrics;

/** Labels a finished job execution. */
public enum Outcome {
    /** The handler returned normally. */
    SUCCESS("success"),
    /** The handler failed and the job will retry. */
    FAILURE("failure"),
    /** The job exhausted its retries. */
    DEAD("dead");

    private final String label;

    Outcome(String label) {
        this.label = label;
    }

    /** The metric label value. */
    public String label() {
        return label;
    }

    @Override
    public String toString() {
        return label;
    }
}
