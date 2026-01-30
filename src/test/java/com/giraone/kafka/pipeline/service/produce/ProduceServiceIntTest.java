package com.giraone.kafka.pipeline.service.produce;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractKafkaIntTest;
import com.giraone.kafka.pipeline.service.CounterService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

abstract class ProduceServiceIntTest extends AbstractKafkaIntTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceServiceIntTest.class);

    @Autowired
    ApplicationProperties applicationProperties;
    @Autowired
    CounterService counterService;

    private Consumer<String, String> consumer;

    @BeforeEach
    protected void setUp() {
        LOGGER.debug("ProduceFlatMapServiceIntTest.setUp");
        createNewTopic(applicationProperties.getTopicA());
        consumer = createConsumer(applicationProperties.getTopicA());
        LOGGER.info("Consumer for \"{}\" created. Assignments = {}", applicationProperties.getTopicA(), consumer.assignment());
    }

    @AfterEach
    public void tearDown() {
        if (consumer != null) {
            consumer.close();
            LOGGER.info("Consumer for \"{}\" closed.", applicationProperties.getTopicA());
        }
    }

    @Test
    void eventsAreProduced() {

        // When the test is started, the events are already sent by the producer
        assertThat(counterService.getCounterProduced()).isGreaterThan(0L);
        assertThat(counterService.getCounterSent()).isGreaterThan(0L);
        waitForMessages(consumer, null); // the created number does not matter
        List<ConsumerRecord<String, String>> records = getAllConsumerRecords();
        assertThat(records).hasSizeGreaterThan(0);
        records.forEach(consumerRecord -> {
            LOGGER.info("{} -> {}", consumerRecord.key(), consumerRecord.value());
            assertThat(consumerRecord.key()).isNotNull();
            assertThat(consumerRecord.value()).isNotNull();
        });
    }
}