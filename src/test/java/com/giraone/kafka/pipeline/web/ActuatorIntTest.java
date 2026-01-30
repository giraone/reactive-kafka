package com.giraone.kafka.pipeline.web;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Test the actuator end points.
 */
@ExtendWith(SpringExtension.class) // for JUnit 5
@SpringBootTest()
@AutoConfigureWebTestClient
class ActuatorIntTest {

    @Autowired
    private WebTestClient webTestClient;

    @DisplayName("Test GET /actuator/health")
    @Test
    void healthIsUp() {

        // act / assert
        webTestClient.get().uri("/actuator/health")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON)
            .expectBody()
            .json("{\"status\":\"UP\"}");
    }

    @DisplayName("Test GET /actuator/metrics")
    @Test
    void metricsIsAvailable() {

        // act / assert
        webTestClient.get().uri("/actuator/metrics")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON)
            .expectBody()
            .jsonPath("names").isNotEmpty();
    }
}
