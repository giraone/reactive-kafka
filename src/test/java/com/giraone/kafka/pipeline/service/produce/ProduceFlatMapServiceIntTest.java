package com.giraone.kafka.pipeline.service.produce;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(locations = "classpath:produce/test-produce-flat-map.properties") // must be properties - not yaml
class ProduceFlatMapServiceIntTest extends ProduceServiceIntTest {
}