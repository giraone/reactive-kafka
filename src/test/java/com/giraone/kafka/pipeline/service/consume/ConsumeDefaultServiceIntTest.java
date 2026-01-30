package com.giraone.kafka.pipeline.service.consume;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractKafkaIntTest;
import com.giraone.kafka.pipeline.service.CounterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import reactor.util.function.Tuples;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(locations = "classpath:consume/test-consume-default.properties") // must be properties - not yaml
class ConsumeDefaultServiceIntTest extends AbstractKafkaIntTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeDefaultServiceIntTest.class);

    @Autowired
    ApplicationProperties applicationProperties;
    @Autowired
    CounterService counterService;

    @BeforeEach
    void setUp() {
        LOGGER.debug("ConsumeDefaultServiceIntTest.setUp");
        createNewTopic(applicationProperties.getTopicB());
    }

    @Test
    void receiveOneEvent() throws Exception {

        long before = counterService.getCounterProcessed();
        send(applicationProperties.getTopicB(), Tuples.of("9", "nine")).block(Duration.ofMinutes(1L));
        // We have to wait some time. We use at least the producer request timeout.
        Thread.sleep(requestTimeoutMillis * 2);
        long after = counterService.getCounterProcessed();
        assertThat(after - before).isEqualTo(1);
    }
}
