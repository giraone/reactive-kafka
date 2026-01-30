package com.giraone.kafka.pipeline.config.properties;

import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
// exclude from test coverage
@Generated
public class RetryProperties {

    private static final int DEFAULT_NUMBER_OF_ATTEMPTS = 2;
    private static final int DEFAULT_FIXED_RETRIES_SECONDS = 3;
    private static final int DEFAULT_EXPONENTIAL_RETRIES_SECONDS = 2;

    /**
     * The maximum number of immediate retry attempts to allow.
     * Default = 2.
     */
    private long maxAttempts = DEFAULT_NUMBER_OF_ATTEMPTS;

    /**
     * The minimum Duration for all backoff, when fixed delay backoff is used.
     * Default = 2 seconds.
     */
    private Duration fixedDelay = Duration.ofSeconds(DEFAULT_FIXED_RETRIES_SECONDS);

    /**
     * The minimum Duration for the first backoff, when exponential backoff is used. Must be defined and > 0
     * to use exponential backoff instead of fixed delay.
     */
    private Duration minBackoff;

    public RetryBackoffSpec toRetry() {
        return minBackoff != null && !minBackoff.isNegative()
            ? Retry.backoff(maxAttempts, minBackoff)
            : Retry.fixedDelay(maxAttempts, fixedDelay);
    }

    public static RetryProperties defaultExponential() {
        return new RetryProperties(DEFAULT_NUMBER_OF_ATTEMPTS, null, Duration.ofSeconds(DEFAULT_EXPONENTIAL_RETRIES_SECONDS));
    }

    public static RetryProperties defaultFixed() {
        return new RetryProperties(DEFAULT_NUMBER_OF_ATTEMPTS, Duration.ofSeconds(DEFAULT_FIXED_RETRIES_SECONDS), null);
    }
}
