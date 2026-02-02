package com.giraone.kafka.pipeline.service.consume;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.CounterService;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.util.concurrent.Queues;

import java.time.Duration;

@Service
public class ConsumeSampledService extends AbstractConsumeService {

    public ConsumeSampledService(
        ApplicationProperties applicationProperties,
        ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver,
        CounterService counterService
    ) {
        super(applicationProperties, kafkaReceiver, counterService);
    }

    //------------------------------------------------------------------------------------------------------------------

    @Override
    public Flux<Void> consume() {

        return receive()
            // at this point, we have events from all consumed topics and all their partitions in one single flux
            // we group this by TopicPartition in order to process each partition in its own flux, committing the
            // single partition's events periodically
            .groupBy(receiverRecord -> receiverRecord.receiverOffset().topicPartition())
            .flatMap(partitionFlux ->
                partitionFlux
                    .flatMapSequential(this::process)
                    // Commit the processed records periodically
                    .sample(Duration.ofMillis(250L))
                    .onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, BufferOverflowStrategy.DROP_OLDEST)
                    .concatMap(this::manualCommit)
            );
    }
}