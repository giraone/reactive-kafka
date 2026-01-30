package com.giraone.kafka.pipeline.service.pipe;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.CounterService;
import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaSender;
import org.springframework.stereotype.Service;

@Service
public class PipePartitionedService extends AbstractPipeService {

    public PipePartitionedService(
        ApplicationProperties applicationProperties,
        CounterService counterService,
        KafkaSender<String, String> kafkaSender,
        KafkaReceiver<String, String> kafkaReceiver
    ) {
        super(applicationProperties, counterService, kafkaSender, kafkaReceiver);
    }

    //------------------------------------------------------------------------------------------------------------------

    @Override
    public void start() {

        LOGGER.info("Assembly of {}", this.getClass().getSimpleName());
        this.receive()
            // group by partition to guarantee ordering
            .groupBy(receiverRecord -> receiverRecord.consumerRecord().partition())
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