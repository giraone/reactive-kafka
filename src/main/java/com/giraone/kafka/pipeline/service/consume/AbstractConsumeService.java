package com.giraone.kafka.pipeline.service.consume;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractService;
import com.giraone.kafka.pipeline.service.CounterService;
import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaReceiverRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.RetryBackoffSpec;

import java.util.Collections;

@Service
public abstract class AbstractConsumeService extends AbstractService {

    protected final String topicInput;
    protected final KafkaReceiver<String, String> kafkaReceiver;

    protected AbstractConsumeService(
        ApplicationProperties applicationProperties,
        KafkaReceiver<String, String> kafkaReceiver,
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

    protected Flux<KafkaReceiverRecord<String, String>> receive() {
        final RetryBackoffSpec retryBackoffSpec = applicationProperties.getConsumer().getRetrySpecification().toRetry();
        LOGGER.info("Start reading from topic \"{}\"", topicInput);
        return kafkaReceiver.receiveManual(Collections.singleton(topicInput))
            // kafka consume retries
            .retryWhen(retryBackoffSpec)
            .doOnNext(this::logReceived);
    }

    protected Mono<KafkaReceiverRecord<String, String>> process(KafkaReceiverRecord<String, String> receiverRecord) {
        final ConsumerRecord<String, String> consumerRecord = receiverRecord.consumerRecord();
        // Simple consumer, that only logs
        counterService.logRateReceived(consumerRecord.partition(), consumerRecord.offset());
        // A real consumer would do some meaningful reactive work here, e.g. write the record to a database using R2DBC
        return Mono.just(receiverRecord);
    }

    protected Mono<Void> manualCommit(KafkaReceiverRecord<String, String> receiverRecord) {
        return Mono.fromRunnable(receiverRecord::acknowledge) // commit vs. acknowledge
            .doOnSuccess(unused -> logCommited(receiverRecord))
            .doOnError(this::logCommitError).then();
    }
}