package com.giraone.kafka.pipeline.config;

import com.giraone.kafka.pipeline.config.properties.SpringKafkaProperties;
import io.atleon.kafka.KafkaSender;
import io.atleon.kafka.KafkaSenderOptions;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Configuration
public class KafkaProducerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerConfig.class);

    @Bean
    public KafkaSender<String, String> kafkaSender(SpringKafkaProperties springKafkaProperties) {

        final KafkaProperties.Producer springProducerProperties = springKafkaProperties.getProducer();
        final Map<String, Object> springProducerPropertiesObjectMap = PropertyUtil.buildProducerProperties(springProducerProperties.getProperties());
        final KafkaSenderOptions.Builder<String, String> builder = KafkaSenderOptions.<String, String>newBuilder()
            .producerProperties(springProducerPropertiesObjectMap)
            .producerProperty(BOOTSTRAP_SERVERS_CONFIG, springKafkaProperties.getBootstrapServers())
            .producerProperty(CLIENT_ID_CONFIG, springKafkaProperties.buildClientId())
            .producerProperty(KEY_SERIALIZER_CLASS_CONFIG, springProducerProperties.getKeySerializer().getName())
            .producerProperty(VALUE_SERIALIZER_CLASS_CONFIG, springProducerProperties.getValueSerializer().getName())
            .producerProperty(ACKS_CONFIG, springProducerProperties.getAcks() != null
                ? springProducerProperties.getAcks()
                : "all"
            )
            .producerProperty(BATCH_SIZE_CONFIG, springProducerProperties.getBatchSize() != null
                ? (int) springProducerProperties.getBatchSize().toBytes()
                : 16384
            )
            // Metrics reporter
            .producerProperty(METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());

        if (springKafkaProperties.getJaas().isEnabled()) {
            final SpringKafkaProperties.Properties properties = springKafkaProperties.getProperties();
            final SpringKafkaProperties.Sasl saslProperties = properties.getSasl();
            final String saslJaasConfig = saslProperties.getJaas().getConfig();
            LOGGER.debug("security.protocol={}, sasl.mechanism={}", springKafkaProperties.getSecurity().getProtocol(), saslProperties.getMechanism());
            builder
                .producerProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, springKafkaProperties.getSecurity().getProtocol())
                .producerProperty("sasl.mechanism", saslProperties.getMechanism())
                .producerProperty("sasl.jaas.config", saslJaasConfig);
        }

        final KafkaSenderOptions<String, String> kafkaSenderOptions = builder.build();
        return KafkaSender.create(kafkaSenderOptions);
    }
}