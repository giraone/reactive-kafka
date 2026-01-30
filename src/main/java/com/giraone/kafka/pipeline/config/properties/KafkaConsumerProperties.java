package com.giraone.kafka.pipeline.config.properties;

import io.atleon.core.AcknowledgementQueueMode;
import lombok.Generated;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;

@Setter
@Getter
@NoArgsConstructor
@ToString
// exclude from test coverage
@Generated
public class KafkaConsumerProperties {

    // --- ATLEON properties - START

    /**
     * Sets the mode of acknowledgement queuing for offsets that are allowed to be committed.
     * In STRICT mode, every offset of every received record is made eligible for commit. In
     * COMPACT mode, commitment of any given record's offset may be skipped if a record that is
     * sequentially after it has already been acknowledged.
     */
    private String acknowledgementQueueMode = AcknowledgementQueueMode.STRICT.name();
    /**
     * Configures the maximum amount of time that will be awaited for in-flight records to be
     * acknowledged from a partition during reception termination (cancellation or error). In
     * addition to allowing for further processing completion to be committed, this also
     * provides a delay for any in-flight non-reactive operations to complete upon termination.
     * This is useful if the stream applies blocking operations that may not be interrupted or
     * otherwise terminated due to stream termination. In the presence of such operations, if
     * processing order is important, it is recommended to set this duration at least as long
     * as the longest blocking operation is expected to take.
     */
    private Duration terminationGracePeriod = Duration.ZERO;
    /**
     * Configures the maximum amount of time that will be awaited for in-flight records to be
     * acknowledged from a partition whose assignment is being revoked. The latest acknowledged
     * offsets from such a partition are then used to issue one last commit before
     * processing/rebalancing is allowed to continue. Note that if it is intended for record
     * acknowledgements to be skipped, this should be set to {@link Duration#ZERO zero} such
     * that consumption continuation does not wait on acknowledgement(s) that will never
     * happen.
     */
    private Duration revocationGracePeriod = Duration.ofSeconds(60L); // Legacy Reactor default
    /**
     * Sets the maximum number of "full" polled record batches that may be prefetched (awaiting
     * emission). The notion of "full" batches is related to
     * {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} and is decoupled from the size of batches
     * actually polled, since it is common that such polled batches are not full. Therefore,
     * these two configurations are multiplied by each other to calculate the maximum total
     * number of records that may be prefetched. EDI Defaults to 1.
     */
    private int fullPollRecordsPrefetch = 1;

    // --- ATLEON properties - END

    /**
     * Retries, when inbound flux (consumer) fails.
     * Since in reactive streams an error represents a terminal signal, any error signal emitted in the inbound
     * Flux will cause the subscription to be cancelled and effectively cause the consumer to shut down.
     * This can be mitigated by using this retry.
     */
    private RetryProperties retrySpecification = RetryProperties.defaultFixed();

    /**
     * Specifies how long the service should wait to fetch partition info of a topic.
     */
    private Duration timeoutFetchPartitionInfo = Duration.ofSeconds(30L);

    /**
     * Specifies how long the service should wait between consuming chain restarts.
     * The consuming chain automatically restarts a few times if it crashes.
     * (Check {@code materialize.ConsumerProperties}.)
     */
    private Duration consumingChainRestartInterval = Duration.ofSeconds(60L);

    /**
     * Maximum number of consuming chain restarts before the service gives up,
     * i.e.
     * (Check {@code materialize.ConsumerProperties}.)
     */
    private int consumingChainMaxRestarts = 10;
}
