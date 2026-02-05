package com.giraone.kafka.pipeline.config;

import com.giraone.kafka.pipeline.config.properties.KafkaConsumerProperties;
import com.giraone.kafka.pipeline.config.properties.SpringKafkaProperties;
import io.atleon.core.AcknowledgementQueueMode;
import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaReceiverOptions;
import io.atleon.kafka.ReactiveAdmin;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public KafkaReceiverOptions<String, String> kafkaReceiverOptions(ApplicationProperties applicationProperties,
                                                                     SpringKafkaProperties springKafkaProperties) {

        final KafkaProperties.Consumer springConsumerProperties = springKafkaProperties.getConsumer();
        final Map<String, Object> springConsumerPropertiesObjectMap = PropertyUtil.buildConsumerProperties(springConsumerProperties.getProperties());
        final KafkaReceiverOptions.Builder<String, String> builder = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(springConsumerPropertiesObjectMap)
            .consumerProperty(BOOTSTRAP_SERVERS_CONFIG, springKafkaProperties.getBootstrapServers())
            .consumerProperty(CLIENT_ID_CONFIG, springKafkaProperties.buildClientId())
            .consumerProperty(GROUP_ID_CONFIG, springKafkaProperties.getConsumer().getGroupId())
            // CooperativeStickyAssignor is best practice and our default
            .consumerProperty(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName())
            .consumerProperty(KEY_DESERIALIZER_CLASS_CONFIG, springConsumerProperties.getKeyDeserializer() != null
                ? springConsumerProperties.getKeyDeserializer().getName()
                : org.apache.kafka.common.serialization.StringDeserializer.class.getName()
            )
            .consumerProperty(VALUE_DESERIALIZER_CLASS_CONFIG, springConsumerProperties.getValueDeserializer() != null
                ? springConsumerProperties.getValueDeserializer().getName()
                : org.apache.kafka.common.serialization.StringDeserializer.class.getName()
            )
            .consumerProperty(AUTO_OFFSET_RESET_CONFIG, springConsumerProperties.getAutoOffsetReset())
            // Poll properties
            .consumerProperty(MAX_POLL_RECORDS_CONFIG, springConsumerProperties.getMaxPollRecords() != null
                ? springConsumerProperties.getMaxPollRecords()
                : 10 // Kafka default = DEFAULT_MAX_POLL_RECORDS=500. Our default FOR AGENTS = 10.
            )
            .consumerProperty(MAX_POLL_INTERVAL_MS_CONFIG, springConsumerProperties.getMaxPollInterval() != null
                ? (int) springConsumerProperties.getMaxPollInterval().toMillis()
                : (int) Duration.ofMinutes(5).toMillis() // Kafka default
            )
            // Fetch properties
            .consumerProperty(FETCH_MAX_WAIT_MS_CONFIG, springConsumerProperties.getFetchMaxWait() != null
                ? (int) springConsumerProperties.getFetchMaxWait().toMillis()
                : DEFAULT_FETCH_MAX_WAIT_MS
            )
            // Metrics reporter - we want default Kafka metrics
            .consumerProperty(METRIC_REPORTER_CLASSES_CONFIG, ReactorKafkaMetricsExporter.class.getName());

        if (springKafkaProperties.getJaas().isEnabled()) {
            final SpringKafkaProperties.Properties properties = springKafkaProperties.getProperties();
            final SpringKafkaProperties.Sasl saslProperties = properties.getSasl();
            final String saslJaasConfig = saslProperties.getJaas().getConfig();
            LOGGER.debug("security.protocol={}, sasl.mechanism={}",
                springKafkaProperties.getSecurity().getProtocol(), saslProperties.getMechanism());
            builder
                .consumerProperty(SECURITY_PROTOCOL_CONFIG, springKafkaProperties.getSecurity().getProtocol())
                .consumerProperty("sasl.mechanism", saslProperties.getMechanism())
                .consumerProperty("sasl.jaas.config", saslJaasConfig);
        }

        // Atleon enhancements
        final KafkaConsumerProperties agentConsumerProperties = applicationProperties.getConsumer();
        builder.acknowledgementQueueMode(AcknowledgementQueueMode.valueOf(agentConsumerProperties.getAcknowledgementQueueMode()));
        builder.terminationGracePeriod(applicationProperties.getConsumer().getTerminationGracePeriod());
        builder.revocationGracePeriod(applicationProperties.getConsumer().getRevocationGracePeriod());
        builder.fullPollRecordsPrefetch(applicationProperties.getConsumer().getFullPollRecordsPrefetch());
        return builder.build();
    }

    @Bean
    public KafkaReceiver<String, String> kafkaReceiver(KafkaReceiverOptions<String, String> kafkaReceiverOptions) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("pollTimeout={}, loadMaxPollRecords={}, maxActiveInFlight={}, maxCommitAttempts={}",
                kafkaReceiverOptions.pollTimeout(), kafkaReceiverOptions.loadMaxPollRecords(),
                kafkaReceiverOptions.maxActiveInFlight(), kafkaReceiverOptions.maxCommitAttempts());
        }
        return KafkaReceiver.create(kafkaReceiverOptions);
    }

    @Bean
    public ReactiveAdmin reactiveAdmin(SpringKafkaProperties springKafkaProperties) {

        final Map<String, Object> adminProperties = new HashMap<>();
        adminProperties.put(BOOTSTRAP_SERVERS_CONFIG, springKafkaProperties.getBootstrapServers());
        adminProperties.put(CLIENT_ID_CONFIG, springKafkaProperties.buildClientId() + "-ADMIN");

        if (springKafkaProperties.getJaas().isEnabled()) {
            final SpringKafkaProperties.Properties properties = springKafkaProperties.getProperties();
            final SpringKafkaProperties.Sasl saslProperties = properties.getSasl();
            final String saslJaasConfig = saslProperties.getJaas().getConfig();
            LOGGER.debug("ADMIN security.protocol={}, sasl.mechanism={}",
                springKafkaProperties.getSecurity().getProtocol(), saslProperties.getMechanism());
            adminProperties.put(SECURITY_PROTOCOL_CONFIG, springKafkaProperties.getSecurity().getProtocol());
            adminProperties.put("sasl.mechanism", saslProperties.getMechanism());
            adminProperties.put("sasl.jaas.config", saslJaasConfig);
        }
        return ReactiveAdmin.create(adminProperties);
    }
}
