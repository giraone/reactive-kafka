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

import java.time.Duration;
import java.util.Locale;

@Service
public abstract class AbstractConsumeService extends AbstractService {

    protected final String topicInput;
    protected final ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver;
    protected final Duration delay; // How long does the pure processing take?

    protected AbstractConsumeService(
        ApplicationProperties applicationProperties,
        ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver,
        CounterService counterService
    ) {
        super(applicationProperties, counterService);
        this.topicInput = applicationProperties.getTopicB();
        this.kafkaReceiver = kafkaReceiver;
        this.delay = applicationProperties.getProcessing().getWaitTime();
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
        LOGGER.info("{}: Start reading from topic \"{}\"", getClass().getSimpleName(), topicInput);
        return kafkaReceiver.receive()
            // kafka consume retries
            .retryWhen(retryBackoffSpec)
            .doOnNext(this::logReceived);
    }

    /**
     * The consumer task, that may take some time (defined by APPLICATION_PROCESSING_TIME) for processing an input.
     */
    protected Mono<ReceiverRecord<String, String>> process(ReceiverRecord<String, String> inputRecord) {
        return Mono.delay(this.delay)
            .map(result -> coreProcess(inputRecord))
            .doOnError(throwable -> LOGGER.error("Error processing record from topic \"{}\" with key={}",
                inputRecord.topic(), inputRecord.key(), throwable))
            .doOnNext(this::logProcessed);
    }

    /**
     * The core consumer task without additional waiting time.
     * Here a simple convert toUpperCase and some logging.
     */
    protected ReceiverRecord<String, String> coreProcess(ReceiverRecord<String, String> receiverRecord) {
        final String input = receiverRecord.value();
        // A real consumer would do some meaningful reactive work here, e.g. write the record to a database using R2DBC
        final String ignored = input.toUpperCase(Locale.ROOT);
        return receiverRecord;
    }

    protected Mono<Void> manualCommit(ReceiverRecord<String, String> receiverRecord) {
        return receiverRecord.receiverOffset().commit() // commit vs. acknowledge
            .doOnSuccess(unused -> logCommited(receiverRecord))
            .doOnError(this::logCommitError).then();
    }
}