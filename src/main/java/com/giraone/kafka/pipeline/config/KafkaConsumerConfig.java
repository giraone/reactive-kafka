package com.giraone.kafka.pipeline.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.MicrometerConsumerListener;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private final String topicInput;

    public KafkaConsumerConfig(ApplicationProperties applicationProperties) {
        topicInput = applicationProperties.getTopicA();
    }

    @Bean
    public ReceiverOptions<String, String> receiverOptions(KafkaProperties springKafkaProperties, MeterRegistry meterRegistry) {

        final Map<String, Object> springConsumerPropertiesObjectMap = springKafkaProperties.buildConsumerProperties();
        // StickyAssignor is best practice and our default
        springConsumerPropertiesObjectMap.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        final ReceiverOptions<String, String> basicReceiverOptions = ReceiverOptions.create(springConsumerPropertiesObjectMap);
        return basicReceiverOptions
            .consumerListener(new MicrometerConsumerListener(meterRegistry)) // we want standard Kafka metrics
            .subscription(List.of(topicInput));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver(ReceiverOptions<String, String> kafkaReceiverOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("pollTimeout={}, maxCommitAttempts={}",
                kafkaReceiverOptions.pollTimeout(), kafkaReceiverOptions.maxCommitAttempts());
        }
        return new ReactiveKafkaConsumerTemplate<>(kafkaReceiverOptions);
    }
}
