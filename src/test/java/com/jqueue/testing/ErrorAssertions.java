package com.jqueue.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

import com.jqueue.common.errors.AppError;

/** Asserts on domain error codes, which is the contract a client actually branches on. */
public final class ErrorAssertions {

    private ErrorAssertions() {
    }

    /** Runs {@code call} and asserts it throws an {@link AppError} carrying {@code code}. */
    public static AppError assertCode(ThrowingCallable call, String code) {
        Throwable thrown = catchThrowable(call);
        assertThat(thrown).as("expected an AppError with code %s", code).isInstanceOf(AppError.class);
        AppError error = (AppError) thrown;
        assertThat(error.code()).isEqualTo(code);
        return error;
    }
}
