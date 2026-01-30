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
     * Concurrency level for processing consumed records within each partition using flatMapSequential.
     * This controls how many records from a single partition are processed in parallel.
     * Default is 1 (sequential processing per partition), which ensures ordering within partitions.
     * Higher values increase throughput but may break ordering guarantees within a partition.
     */
    private int concurrencyPerPartition = 1;

    /**
     * ONLY for ORDERED processing! Reduce the concurrency of each agent instance by using modulo on the partition index.
     * This value must be either null (no reducing) or between 1 and the number of partitions of the schedule topic(s).
     */
    private Integer reducedPartitionGroupBy;


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
