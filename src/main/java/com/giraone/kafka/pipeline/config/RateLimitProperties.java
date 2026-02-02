package com.giraone.kafka.pipeline.config;

import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Rate limiting properties for processing consumed records. Not yet used.
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
// exclude from test coverage
@Generated
public class RateLimitProperties {

    /**
     * Configures the permissions limit for a period.
     */
    private int limit = 0;

    /**
     * Configures the period of limit. Default value is 1 second.
     */
    private long periodMs = 1000L;

    /**
     * Configures the timeout duration, after which events are emitted even if less than {@see limit} events arrived
     */
    private long timeoutMs = 2000L;
}
