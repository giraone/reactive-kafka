package com.giraone.kafka.pipeline.service.consume;

import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(locations = "classpath:consume/test-consume-sampled.properties") // must be properties - not yaml
class ConsumeSampledServiceIntTest extends ConsumerServiceIntTest {
}
