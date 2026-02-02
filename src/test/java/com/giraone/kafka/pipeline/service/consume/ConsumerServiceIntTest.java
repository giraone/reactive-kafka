package com.giraone.kafka.pipeline.service.consume;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractKafkaIntTest;
import com.giraone.kafka.pipeline.service.CounterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

abstract class ConsumerServiceIntTest extends AbstractKafkaIntTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerServiceIntTest.class);

    @Autowired
    ApplicationProperties applicationProperties;
    @Autowired
    CounterService counterService;

    @BeforeEach
    void setUp() {
        LOGGER.debug("ConsumerServiceIntTest.setUp");
        createNewTopic(applicationProperties.getTopicB());
    }

    @Test
    void receiveOneEvent() throws Exception {

        long beforeReceived = counterService.getCounterReceived();
        long beforeProcessed = counterService.getCounterProcessed();
        long beforeCommitted = counterService.getCounterCommitted();
        send(applicationProperties.getTopicB(), Tuples.of("9", "nine"));
        // We have to wait some time. We use at least the producer request timeout.
        Thread.sleep(REQUEST_TIMEOUT_MILLIS);
        long afterReceived = counterService.getCounterReceived();
        long afterProcessed = counterService.getCounterProcessed();
        long afterCommitted = counterService.getCounterCommitted();
        assertThat(afterReceived - beforeReceived).isEqualTo(1);
        assertThat(afterProcessed - beforeProcessed).isEqualTo(1);
        assertThat(afterCommitted - beforeCommitted).isEqualTo(1);
    }

    @Test
    void receiveMultipleEvents() throws Exception {

        int count = 10;
        long beforeReceived = counterService.getCounterReceived();
        long beforeProcessed = counterService.getCounterProcessed();
        long beforeCommitted = counterService.getCounterCommitted();
        for (int i = 0; i < count; i++) {
            send(applicationProperties.getTopicB(), Tuples.of(String.format("%d", i), String.format("%04d", i)));
        }
        // We have to wait some time. We use at least the producer request timeout.
        Thread.sleep(REQUEST_TIMEOUT_MILLIS);
        long afterReceived = counterService.getCounterReceived();
        long afterProcessed = counterService.getCounterProcessed();
        long afterCommitted = counterService.getCounterCommitted();
        assertThat(afterReceived - beforeReceived).isEqualTo(count);
        assertThat(afterProcessed - beforeProcessed).isEqualTo(count);
        // No differentiation between COMPACT/STRICT like in atleon-kafka
        assertThat(afterCommitted - beforeCommitted).isEqualTo(count);
    }
}
