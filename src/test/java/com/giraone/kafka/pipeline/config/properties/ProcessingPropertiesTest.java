package com.giraone.kafka.pipeline.config.properties;

import com.giraone.kafka.pipeline.config.ProcessingProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.scheduler.Scheduler;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessingPropertiesTest {

    @ParameterizedTest
    @CsvSource(value = {
        "parallel||Schedulers.parallel()",
        "newParallel||parallel(8,\"newParallelConsumer\")",
        "newParallel|2|parallel(2,\"newParallelConsumer\")"
    }, delimiterString = "|")
    void buildSchedulerParallel(String schedulerType, Integer concurrency, String expectedToString) {

        ProcessingProperties processingProperties = new ProcessingProperties();
        processingProperties.setSchedulerType(schedulerType);
        if (concurrency != null) {
            processingProperties.setNewParallelPoolSize(concurrency);
        }
        Scheduler scheduler = processingProperties.buildScheduler();
        assertThat(scheduler.toString()).startsWith(expectedToString);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "newBoundedElastic|||boundedElastic(\"newElasticConsumer\",maxThreads=8,maxTaskQueuedPerThread=256,ttl=60s",
        "newBoundedElastic|2|3|boundedElastic(\"newElasticConsumer\",maxThreads=2,maxTaskQueuedPerThread=3,ttl=60s",
    }, delimiterString = "|")
    void buildSchedulerBoundedElastic(String schedulerType, Integer size, Integer queueSize, String expectedToString) {

        ProcessingProperties processingProperties = new ProcessingProperties();
        processingProperties.setSchedulerType(schedulerType);
        if (size != null) {
            processingProperties.setNewBoundedElasticSize(size);
        }
        if (queueSize != null) {
            processingProperties.setNewBoundedElasticQueueSize(queueSize);
        }
        Scheduler scheduler = processingProperties.buildScheduler();
        assertThat(scheduler.toString()).startsWith(expectedToString);
    }
}