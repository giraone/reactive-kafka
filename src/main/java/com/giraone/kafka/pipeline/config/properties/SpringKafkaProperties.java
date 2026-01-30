package com.giraone.kafka.pipeline.config.properties;

import lombok.Generated;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * A clone of org.springframework.boot.autoconfigure.kafka.KafkaProperties!
 * The intention is to re-use the same property names as Spring Boot, but avoid
 * the dependency on the entire Spring Boot Kafka auto-configuration module.
 * Currently only a subset of properties needed for Kafka connectivity and configuration.
 * Example:
 * <code>
 * spring:
 * kafka:
 * bootstrap-servers: 'localhost:9991,localhost:9992,localhost:9993,localhost:9994,localhost:9995,localhost:9996'
 * client-id: 'my-client-id'
 * consumer:
 * group-id: 'my-group-id'
 * jaas:
 * enabled: true
 * security:
 * protocol: SASL_PLAINTEXT
 * properties:
 * sasl:
 * mechanism: SCRAM-SHA-512
 * jaas-config: org.apache.kafka.common.security.scram.ScramLoginModule required username='user' password='secret';
 * </code>
 */
@ConfigurationProperties(prefix = "spring.kafka")
@Setter
@Getter
@NoArgsConstructor
@ToString
// exclude from test coverage
@Generated
public class SpringKafkaProperties {

    /**
     * Kafka cluster. Default = "localhost:9092".
     */
    private String bootstrapServers = "localhost:9092";
    /**
     * Kafka client id. Will be suffixed with "-" and the CF_INSTANCE_INDEX.
     */
    private String clientId = "agent-wrapper";

    private Jaas jaas = new Jaas();
    private KafkaProperties.Security security = new KafkaProperties.Security();
    private Properties properties = new Properties();
    private KafkaProperties.Consumer consumer = new KafkaProperties.Consumer();
    private KafkaProperties.Producer producer = new KafkaProperties.Producer();

    /**
     * Build a client id from the provided prefix and the CF_INSTANCE_INDEX, if available.
     *
     * @return client id
     */
    public String buildClientId() {
        return buildClientId(System.getenv("CF_INSTANCE_INDEX"));
    }

    public String buildClientId(String suffix) {
        return suffix != null && !suffix.trim().isEmpty()
            ? (clientId + "-" + suffix.trim())
            : clientId;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    @ToString
    // exclude from test coverage
    @Generated
    public static class Properties {
        private Sasl sasl = new Sasl();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    @ToString
    // exclude from test coverage
    @Generated
    public static class Jaas {
        private boolean enabled;
        private String controlFlag;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    @ToString
    // exclude from test coverage
    @Generated
    public static class Sasl {
        private String mechanism = "PLAINTEXT";
        private JaasConfig jaas;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    @ToString
    // exclude from test coverage
    @Generated
    public static class JaasConfig {
        private String config;
    }
}
