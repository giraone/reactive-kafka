package com.giraone.kafka.pipeline.service.consume;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.CounterService;
import io.atleon.kafka.KafkaReceiver;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class ConsumeDefaultService extends AbstractConsumeService {

    public ConsumeDefaultService(
        ApplicationProperties applicationProperties,
        KafkaReceiver<String, String> kafkaReceiver,
        CounterService counterService
    ) {
        super(applicationProperties, kafkaReceiver, counterService);
    }

    //------------------------------------------------------------------------------------------------------------------

    @Override
    public Flux<Void> consume() {

        return receive()
            .flatMapSequential(this::process)
            .doOnNext(this::logProcessed)
            .concatMap(this::manualCommit);
    }
}