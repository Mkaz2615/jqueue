package com.jqueue.common.errors;

/**
 * Classifies an error so that outer layers can map it onto a protocol-specific status
 * without knowing anything about the originating domain.
 */
public enum ErrorKind {
    /** An unexpected failure. Maps to 5xx. */
    INTERNAL("internal"),
    /** A caller-supplied value that violates an invariant. */
    INVALID_ARGUMENT("invalid_argument"),
    /** A lookup that yielded nothing. */
    NOT_FOUND("not_found"),
    /** A violated uniqueness or state precondition. */
    CONFLICT("conflict"),
    /** A downstream dependency that is temporarily unusable. */
    UNAVAILABLE("unavailable");

    private final String label;

    ErrorKind(String label) {
        this.label = label;
    }

    /** Renders the kind for logs. */
    public String label() {
        return label;
    }
}
