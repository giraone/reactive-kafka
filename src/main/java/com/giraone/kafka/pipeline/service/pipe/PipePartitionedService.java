package com.giraone.kafka.pipeline.service.pipe;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.CounterService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

@Service
public class PipePartitionedService extends AbstractPipeService {

    public PipePartitionedService(
        ApplicationProperties applicationProperties,
        CounterService counterService,
        ReactiveKafkaProducerTemplate<String, String> kafkaSender,
        ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver
    ) {
        super(applicationProperties, counterService, kafkaSender, kafkaReceiver);
    }

    //------------------------------------------------------------------------------------------------------------------

    @Override
    public void start() {

        LOGGER.info("Assembly of {}", this.getClass().getSimpleName());
        this.receive()
            // group by partition to guarantee ordering
            .groupBy(ConsumerRecord::partition)
            .flatMap(partitionFlux ->
                partitionFlux.publishOn(scheduler)
                    // perform the pipe task
                    .flatMapSequential(this::process)
                    // send result to target topic
                    .concatMap(this::send)
                    // commit every processed record in strict order
                    .concatMap(this::manualCommit)
            )
            // log any error
            .doOnError(e -> counterService.logError("PipePartitionedService failed!", e))
            // subscription main loop - restart on unhandled errors
            .subscribe(null, this::restartMainLoopOnError);
        counterService.logMainLoopStarted(getClass().getSimpleName());
    }
}