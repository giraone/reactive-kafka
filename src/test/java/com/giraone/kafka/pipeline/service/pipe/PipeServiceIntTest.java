package com.giraone.kafka.pipeline.service.pipe;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractKafkaIntTest;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.util.function.Tuples;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class PipeServiceIntTest extends AbstractKafkaIntTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipeServiceIntTest.class);

    @Autowired
    ApplicationProperties applicationProperties;

    private Consumer<String, String> consumer;

    @BeforeEach
    protected void setUp() {
        LOGGER.debug("PipeServiceIntTest.setUp");
        createNewTopic(applicationProperties.getTopicA());
        createNewTopic(applicationProperties.getTopicB());
        consumer = createConsumer(applicationProperties.getTopicB());
        LOGGER.info("Consumer for \"{}\" created.", applicationProperties.getTopicB());
    }

    @AfterEach
    public void tearDown() {
        // No consumer close
    }

    @Test
    void passMultipleEvents() throws Exception {

        this.sendMessagesAndAssertReceived(
            applicationProperties.getTopicA(), consumer,
            List.of(Tuples.of("key1", "Eins"), Tuples.of("key2", "Zwei"), Tuples.of("key3", "Drei")),
            List.of(Tuples.of("key1", "EINS"), Tuples.of("key2", "ZWEI"), Tuples.of("key3", "DREI"))
        );
    }
}
