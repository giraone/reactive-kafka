package com.giraone.kafka.pipeline.config;

import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * Processing properties for consumer and pipe processing.
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
// exclude from test coverage
@Generated
public class ProcessingProperties {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingProperties.class);

    /**
     * Type of scheduler used for processing records - either "parallel", "newParallel" or "newBoundedElastic".
     * <ul>
     *     <li><b>newParallel</b>: For reactive (non-blocking) agents. Creates a fixed-size thread pool without daemon threads.
     *         Recommended for agents with async I/O operations.</li>
     *     <li><b>newBoundedElastic</b>: For imperative (blocking) agents. Required when the processing chain contains
     *         blocking operations (e.g., blocking SFTP calls). Has a bounded queue to prevent unbounded memory growth.</li>
     *     <li><b>parallel</b>: Legacy option using Reactor's default parallel scheduler.</li>
     * </ul>
     * Default is "newParallel".
     */
    private String schedulerType = "newParallel";

    /**
     * Pool size for the newParallel scheduler.
     * Default is 8, oriented on cores available in cf.
     */
    private int newParallelPoolSize = 8;

    /**
     * Thread pool size for the newBoundedElastic scheduler.
     * Default is 8, oriented on cores available in cf.
     */
    private int newBoundedElasticSize = 8;

    /**
     * Queue size for the newBoundedElastic scheduler.
     * Default is 256 (8 * 32).
     */
    private int newBoundedElasticQueueSize = 8 * 32;

    /**
     * Processing time for consume and pipe service. The transform step will wait additionally this amount of time.
     * Default is 10ms.
     */
    private Duration waitTime = Duration.ofMillis(10);

    /**
     * Rate limiter properties for processing consumed records. Not used yet.
     */
    private RateLimitProperties rate = new RateLimitProperties();

    /**
     * Build a scheduler. Scheduler type and concurrency are fetch from the corresponding properties.
     *
     * @return A newly created Scheduler ("newParallel", "newBoundedElastic") or a default one ("parallel").
     */
    public Scheduler buildScheduler() {

        final Scheduler ret;
        if ("newParallel".equalsIgnoreCase(schedulerType)) {
            // no daemon threads, so we do not create any during a shutdown
            ret = Schedulers.newParallel("newParallelConsumer", newParallelPoolSize, false);
        } else if ("newBoundedElastic".equalsIgnoreCase(schedulerType)) {
            ret = Schedulers.newBoundedElastic(newBoundedElasticSize, newBoundedElasticQueueSize,
                "newElasticConsumer");
        } else {
            ret = Schedulers.parallel();
        }
        LOGGER.debug("Using {} as scheduler for Kafka consumer", ret);
        return ret;
    }
}
