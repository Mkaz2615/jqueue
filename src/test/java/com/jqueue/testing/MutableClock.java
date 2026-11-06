package com.jqueue.testing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** A clock whose time is advanced explicitly, so scheduling assertions are exact. */
public final class MutableClock extends Clock {

    private volatile Instant current;

    public MutableClock(Instant start) {
        this.current = start;
    }

    public void advance(Duration d) {
        current = current.plus(d);
    }

    public void set(Instant at) {
        current = at;
    }

    @Override
    public Instant instant() {
        return current;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
