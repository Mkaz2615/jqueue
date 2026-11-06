package com.jqueue.common.uid;

/**
 * The port used by the application layer to mint identifiers. It is an interface so that
 * tests can substitute a deterministic sequence.
 */
@FunctionalInterface
public interface IdGenerator {

    /** Returns a new opaque identifier. */
    String newId();
}
