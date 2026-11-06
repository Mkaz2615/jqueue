package com.jqueue.common.errors;

import java.util.Objects;
import java.util.Optional;

/**
 * The transport-agnostic error that flows from the domain and application layers up to the
 * API layer.
 *
 * <p>Lower layers never know about HTTP. They throw an {@code AppError} carrying a stable,
 * machine-readable {@link #code()}; the API exception handler is the single place that maps
 * an {@link ErrorKind} onto a status code.
 */
public final class AppError extends RuntimeException {

    private final ErrorKind kind;
    private final String code;
    private final String safeMessage;

    private AppError(ErrorKind kind, String code, String safeMessage, Throwable cause) {
        super(render(code, safeMessage, cause), cause);
        this.kind = Objects.requireNonNull(kind, "kind");
        this.code = Objects.requireNonNull(code, "code");
        this.safeMessage = Objects.requireNonNull(safeMessage, "message");
    }

    // ------------------------------------------------------------------------------------
    // Factories
    // ------------------------------------------------------------------------------------

    /** Builds an error without a cause. */
    public static AppError of(ErrorKind kind, String code, String message) {
        return new AppError(kind, code, message, null);
    }

    /** Builds an error carrying an underlying cause. */
    public static AppError wrap(ErrorKind kind, String code, String message, Throwable cause) {
        return new AppError(kind, code, message, cause);
    }

    /** Shorthand for an {@link ErrorKind#INVALID_ARGUMENT} error. */
    public static AppError invalid(String code, String message) {
        return of(ErrorKind.INVALID_ARGUMENT, code, message);
    }

    /** Shorthand for an {@link ErrorKind#NOT_FOUND} error. */
    public static AppError notFound(String code, String message) {
        return of(ErrorKind.NOT_FOUND, code, message);
    }

    /** Shorthand for an {@link ErrorKind#CONFLICT} error. */
    public static AppError conflict(String code, String message) {
        return of(ErrorKind.CONFLICT, code, message);
    }

    /** Shorthand for an {@link ErrorKind#INTERNAL} error carrying a cause. */
    public static AppError internal(String code, String message, Throwable cause) {
        return wrap(ErrorKind.INTERNAL, code, message, cause);
    }

    // ------------------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------------------

    /** Drives protocol mapping. */
    public ErrorKind kind() {
        return kind;
    }

    /**
     * A stable identifier defined by the owning aggregate, for example
     * {@code invalid_priority}. Clients may branch on it; it never changes.
     */
    public String code() {
        return code;
    }

    /** A human-readable description that is safe to return to a caller. */
    public String safeMessage() {
        return safeMessage;
    }

    // ------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------

    /**
     * Finds the first {@code AppError} in a cause chain. Empty means the chain holds no
     * AppError, in which case callers should treat the failure as internal.
     */
    public static Optional<AppError> find(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof AppError appError) {
                return Optional.of(appError);
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return Optional.empty();
    }

    /** Reports the kind of an error, defaulting to {@link ErrorKind#INTERNAL}. */
    public static ErrorKind kindOf(Throwable error) {
        return find(error).map(AppError::kind).orElse(ErrorKind.INTERNAL);
    }

    /**
     * Renders any throwable as a one-line message. A message-less exception falls back to
     * its class name, so a recorded failure is never an empty string.
     */
    public static String describe(Throwable error) {
        if (error == null) {
            return "";
        }
        String message = error.getMessage();
        if (message == null || message.isBlank()) {
            return error.getClass().getSimpleName();
        }
        return message;
    }

    private static String render(String code, String message, Throwable cause) {
        if (cause != null) {
            return code + ": " + message + ": " + describe(cause);
        }
        return code + ": " + message;
    }
}
