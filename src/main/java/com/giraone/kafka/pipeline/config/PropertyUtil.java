package com.giraone.kafka.pipeline.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

public final class PropertyUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyUtil.class);

    // Hide
    private PropertyUtil() {
    }

    public static Map<String, Object> buildProducerProperties(Map<String, String> stringObjectMap) {
        return buildMap(stringObjectMap, "producer");
    }

    public static Map<String, Object> buildConsumerProperties(Map<String, String> stringObjectMap) {
        return buildMap(stringObjectMap, "consumer");
    }

    private static Map<String, Object> buildMap(Map<String, String> stringObjectMap, String context) {
        return stringObjectMap.entrySet()
                .stream()
                .peek(entry -> LOGGER.info("using spring.kafka.{}.properties.{}", context, entry))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }
}
