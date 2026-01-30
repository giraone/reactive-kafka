package com.giraone.kafka.pipeline.config.properties;

import lombok.Generated;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.Duration;

@Setter
@Getter
@NoArgsConstructor
@ToString
// exclude from test coverage
@Generated
public class KafkaConsumerProperties {

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
