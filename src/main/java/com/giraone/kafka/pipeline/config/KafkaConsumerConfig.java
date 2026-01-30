package com.giraone.kafka.pipeline.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private final String topicInput;

    public KafkaConsumerConfig(ApplicationProperties applicationProperties) {
        topicInput = applicationProperties.getTopicA();
    }

    @Bean
    public ReceiverOptions<String, String> receiverOptions(ApplicationProperties applicationProperties, KafkaProperties springKafkaProperties) {

        final Map<String, Object> springConsumerPropertiesObjectMap =
            springKafkaProperties.getProperties().entrySet()
                .stream()
                .peek(entry -> LOGGER.info("using spring.kafka.consumer.properties.{}", entry))
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue
                ));

        final ReceiverOptions<String, String> basicReceiverOptions = ReceiverOptions.create(springConsumerPropertiesObjectMap);
        // TODO: Missing properties
        return basicReceiverOptions.subscription(List.of(topicInput));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate(ReceiverOptions<String, String> kafkaReceiverOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("pollTimeout={}, maxCommitAttempts={}",
                kafkaReceiverOptions.pollTimeout(), kafkaReceiverOptions.maxCommitAttempts());
        }
        return new ReactiveKafkaConsumerTemplate<>(kafkaReceiverOptions);
    }
}
