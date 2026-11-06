package com.jqueue.testing;

import java.util.concurrent.atomic.AtomicInteger;

import com.jqueue.common.uid.IdGenerator;

/** Emits predictable identifiers such as {@code job-1}, {@code job-2}. */
public final class SequenceIdGenerator implements IdGenerator {

    private final String prefix;
    private final AtomicInteger next = new AtomicInteger();

    public SequenceIdGenerator(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String newId() {
        return prefix + "-" + next.incrementAndGet();
    }
}
