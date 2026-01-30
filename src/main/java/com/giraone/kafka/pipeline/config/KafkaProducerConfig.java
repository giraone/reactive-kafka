package com.giraone.kafka.pipeline.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.MicrometerProducerListener;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;
import java.util.stream.Collectors;

@Configuration
public class KafkaProducerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerConfig.class);

    @Bean
    public SenderOptions<String, String> senderOptions(KafkaProperties springKafkaProperties, MeterRegistry meterRegistry) {

        final KafkaProperties.Producer springProducerProperties = springKafkaProperties.getProducer();
        final Map<String, Object> springProducerPropertiesObjectMap =
            springProducerProperties.getProperties().entrySet()
                .stream()
                .peek(entry -> LOGGER.info("using spring.kafka.producer.properties.{}", entry))
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue
                ));

        final SenderOptions<String, String> basicSenderOptions = SenderOptions.create(springProducerPropertiesObjectMap);

        return basicSenderOptions
            .producerListener(new MicrometerProducerListener(meterRegistry)) // we want standard Kafka metrics
            ;
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate(SenderOptions<String, String> kafkaSenderOptions) {
        return new ReactiveKafkaProducerTemplate<>(kafkaSenderOptions);
    }
}