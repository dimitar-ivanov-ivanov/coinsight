package coinsight.arbitrage.aggregations.controllers;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;

/**
 * Translates ResponseStatusException (and, if ever needed, other exception types) into a
 * consistent JSON error shape across every endpoint, instead of Spring Boot's default generic
 * error body. The service layer still throws - Java has no clean built-in way to thread
 * "success or a typed error" through return values without something heavier (a Result/Either
 * type), so throwing stays the natural way to signal a validation failure - this class is the
 * one place that turns it into the actual HTTP response a client sees.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Shape of every error response this API returns.
     *
     * @param status  HTTP status code
     * @param error   short machine-readable reason phrase
     * @param message human-readable detail (e.g. which exchange value was invalid)
     * @param timestamp when the error occurred
     */
    public record ErrorResponse(int status, String error, String message, Instant timestamp) {
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ErrorResponse> handleResponseStatusException(ResponseStatusException ex) {
        ErrorResponse body = new ErrorResponse(
                ex.getStatusCode().value(),
                ex.getStatusCode().toString(),
                ex.getReason(),
                Instant.now()
        );
        return ResponseEntity.status(ex.getStatusCode()).body(body);
    }
}
