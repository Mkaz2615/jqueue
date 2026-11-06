package com.jqueue.domain.job.vo;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.JobErrorCodes;

/**
 * The lifecycle position of a Job aggregate.
 *
 * <p>The legal transitions are encoded here, next to the states themselves, so that no
 * service can invent a transition the domain does not permit.
 */
public enum JobState {
    /** Waiting for its process-at instant. */
    SCHEDULED("scheduled"),
    /** Queued and eligible for dequeue. */
    PENDING("pending"),
    /** A worker holds a lease on the job and is executing it. */
    ACTIVE("active"),
    /** Failed and waiting for its next attempt. */
    RETRYING("retrying"),
    /** Finished successfully. */
    COMPLETED("completed"),
    /** Exhausted its retries; sits in the dead-letter queue awaiting an operator. */
    DEAD("dead");

    /** The single source of truth for the lifecycle graph. */
    private static final Map<JobState, Set<JobState>> ALLOWED = new EnumMap<>(JobState.class);

    static {
        ALLOWED.put(SCHEDULED, EnumSet.of(PENDING, DEAD));
        ALLOWED.put(PENDING, EnumSet.of(ACTIVE, DEAD));
        ALLOWED.put(ACTIVE, EnumSet.of(COMPLETED, RETRYING, DEAD, PENDING));
        ALLOWED.put(RETRYING, EnumSet.of(PENDING, DEAD));
        ALLOWED.put(DEAD, EnumSet.of(PENDING));
        ALLOWED.put(COMPLETED, EnumSet.noneOf(JobState.class));
    }

    private final String wire;

    JobState(String wire) {
        this.wire = wire;
    }

    /** Lists every state in lifecycle order, for use by the API layer. */
    public static List<JobState> all() {
        return List.of(values());
    }

    /** Validates and constructs a JobState from its wire representation. */
    public static JobState fromWire(String raw) {
        for (JobState state : values()) {
            if (state.wire.equals(raw)) {
                return state;
            }
        }
        throw AppError.invalid(JobErrorCodes.INVALID_JOB_STATE, "unknown job state: " + raw);
    }

    /** The persisted and wire representation, for example {@code pending}. */
    public String wire() {
        return wire;
    }

    /**
     * Reports whether no further transition out of this state happens automatically. Dead is
     * terminal for the worker pool, but an operator may still requeue it.
     */
    public boolean isTerminal() {
        return this == COMPLETED || this == DEAD;
    }

    /** Reports whether moving to {@code next} is legal. */
    public boolean canTransitionTo(JobState next) {
        return ALLOWED.get(this).contains(next);
    }

    @Override
    public String toString() {
        return wire;
    }
}
