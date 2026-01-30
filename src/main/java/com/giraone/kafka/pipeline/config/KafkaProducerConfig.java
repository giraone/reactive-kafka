package com.giraone.kafka.pipeline.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
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

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public SenderOptions<String, String> senderOptions(KafkaProperties springKafkaProperties, MeterRegistry meterRegistry) {

        final Map<String, Object> springProducerPropertiesObjectMap = springKafkaProperties.buildProducerProperties();
        final SenderOptions<String, String> basicSenderOptions = SenderOptions.create(springProducerPropertiesObjectMap);
        return basicSenderOptions
            .producerListener(new MicrometerProducerListener(meterRegistry)); // we want standard Kafka metrics
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, String> kafkaSender(SenderOptions<String, String> kafkaSenderOptions) {
        return new ReactiveKafkaProducerTemplate<>(kafkaSenderOptions);
    }
}