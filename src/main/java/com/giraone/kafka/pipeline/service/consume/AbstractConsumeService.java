package com.giraone.kafka.pipeline.service.consume;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractService;
import com.giraone.kafka.pipeline.service.CounterService;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.RetryBackoffSpec;

@Service
public abstract class AbstractConsumeService extends AbstractService {

    protected final String topicInput;
    protected final ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver;

    protected AbstractConsumeService(
        ApplicationProperties applicationProperties,
        ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver,
        CounterService counterService
    ) {
        super(applicationProperties, counterService);
        this.topicInput = applicationProperties.getTopicB();
        this.kafkaReceiver = kafkaReceiver;
    }

    //------------------------------------------------------------------------------------------------------------------

    protected abstract Flux<Void> consume();

    protected void start() {
        consume()
            .doOnError(e -> counterService.logError("ConsumeDefaultService failed!", e))
            .subscribe(null, counterService::logMainLoopError);
    }

    protected Flux<ReceiverRecord<String, String>> receive() {
        final RetryBackoffSpec retryBackoffSpec = applicationProperties.getConsumer().getRetrySpecification().toRetry();
        LOGGER.info("Start reading from topic \"{}\"", topicInput);
        return kafkaReceiver.receive()
            // kafka consume retries
            .retryWhen(retryBackoffSpec)
            .doOnNext(this::logReceived);
    }

    protected Mono<ReceiverRecord<String, String>> process(ReceiverRecord<String, String> receiverRecord) {
        // Simple consumer, that only logs
        counterService.logRateReceived(receiverRecord.partition(), receiverRecord.offset());
        // A real consumer would do some meaningful reactive work here, e.g. write the record to a database using R2DBC
        return Mono.just(receiverRecord);
    }

    protected Mono<Void> manualCommit(ReceiverRecord<String, String> receiverRecord) {
        return receiverRecord.receiverOffset().commit() // commit vs. acknowledge
            .doOnSuccess(unused -> logCommited(receiverRecord))
            .doOnError(this::logCommitError).then();
    }
}