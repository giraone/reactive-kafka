package com.giraone.kafka.pipeline.service.produce;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.CounterService;
import io.atleon.kafka.KafkaSender;
import io.atleon.kafka.KafkaSenderRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

@Service
public class ProduceConcatMapService extends AbstractProduceService {

    public ProduceConcatMapService(
        ApplicationProperties applicationProperties,
        CounterService counterService,
        KafkaSender<String, String> kafkaSender
    ) {
        super(applicationProperties, counterService, kafkaSender);
    }

    //------------------------------------------------------------------------------------------------------------------

    @Override
    public void start() {

        LOGGER.info("STARTING to produce {} events using ProduceConcatMapService.", maxNumberOfEvents);
        final long start = System.currentTimeMillis();
        source(applicationProperties.getProducerVariables().getInterval(), maxNumberOfEvents)
            // A scheduler is needed - a single or parallel(1) is OK
            .publishOn(schedulerForKafkaProduce)
            .concatMap(tuple -> {
                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicOutput, tuple.getT1(), tuple.getT2());
                final KafkaSenderRecord<String, String, String> senderRecord = KafkaSenderRecord.create(producerRecord, tuple.getT1());
                return this.send(senderRecord);
            })
            .doOnError(e -> counterService.logError("ProduceConcatMapService failed!", e))
            .subscribe(null, counterService::logMainLoopError, () -> LOGGER.info("Finished producing {} events to {} after {} seconds",
                maxNumberOfEvents, topicOutput, (System.currentTimeMillis() - start) / 1000L));
        counterService.logMainLoopStarted(getClass().getSimpleName());
    }
}