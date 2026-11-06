package com.jqueue.api.middleware;

import java.util.Locale;
import java.util.stream.Collectors;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import com.jqueue.common.errors.AppError;
import com.jqueue.common.errors.ErrorKind;

/**
 * Converts exceptions into HTTP responses.
 *
 * <p>This is the only place in the codebase that knows about status codes. Controllers
 * call a service and let any {@link AppError} propagate, so no controller can invent its
 * own mapping and no domain class ever has to know that something "was a 404".
 *
 * <p>It is also the crash barrier for the HTTP layer: an unexpected exception becomes a 500
 * with a generic message. The stack trace goes to the log, never to the response — it tells
 * an attacker the file layout and dependency versions, and tells a legitimate client
 * nothing it can act on.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    static final String INVALID_REQUEST = "invalid_request";

    @ExceptionHandler(AppError.class)
    public ResponseEntity<ErrorResponse> handleAppError(AppError error, HttpServletRequest request) {
        HttpStatus status = statusFor(error.kind());
        if (status.is5xxServerError()) {
            log.atError()
                    .addKeyValue("path", request.getRequestURI())
                    .addKeyValue("code", error.code())
                    .addKeyValue("error", error.getMessage())
                    .log("request failed");
        }
        return ResponseEntity.status(status).body(new ErrorResponse(error.code(), error.safeMessage()));
    }

    /** A request body that failed bean validation. The message names every offending field. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleInvalidBody(MethodArgumentNotValidException error) {
        String message = error.getBindingResult().getFieldErrors().stream()
                .map(GlobalExceptionHandler::describeField)
                .sorted()
                .collect(Collectors.joining("; "));
        return badRequest(message.isEmpty() ? "request validation failed" : message);
    }

    /** Malformed JSON, or a field of the wrong type. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadableBody(HttpMessageNotReadableException error) {
        return badRequest("request body is missing, is not valid JSON, or has a field of the wrong type");
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException error) {
        return badRequest(error.getName() + " has an invalid value");
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingParameter(MissingServletRequestParameterException error) {
        return badRequest(error.getParameterName() + " is required");
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMediaType(HttpMediaTypeNotSupportedException error) {
        return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                .body(new ErrorResponse("unsupported_media_type", "request body must be application/json"));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(NoResourceFoundException error) {
        String path = error.getResourcePath();
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("not_found", "no route matches " + (path.startsWith("/") ? path : "/" + path)));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethod(HttpRequestMethodNotSupportedException error) {
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED)
                .body(new ErrorResponse("method_not_allowed", error.getMethod() + " is not allowed on this route"));
    }

    /** The crash barrier: anything unexpected is a 500 with a generic message. */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception error, HttpServletRequest request) {
        log.atError()
                .setCause(error)
                .addKeyValue("path", request.getRequestURI())
                .log("unhandled error");
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("internal_error", "an unexpected error occurred"));
    }

    /** Maps a domain error kind onto an HTTP status code. */
    static HttpStatus statusFor(ErrorKind kind) {
        return switch (kind) {
            case INVALID_ARGUMENT -> HttpStatus.BAD_REQUEST;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case CONFLICT -> HttpStatus.CONFLICT;
            case UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE;
            case INTERNAL -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }

    private static ResponseEntity<ErrorResponse> badRequest(String message) {
        return ResponseEntity.badRequest().body(new ErrorResponse(INVALID_REQUEST, message));
    }

    /** Reports a field by its JSON name, which is the name the client actually sent. */
    private static String describeField(FieldError error) {
        return toSnakeCase(error.getField()) + " " + error.getDefaultMessage();
    }

    static String toSnakeCase(String camel) {
        return camel.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(Locale.ROOT);
    }
}
