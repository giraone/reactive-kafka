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

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;

@Service
public abstract class AbstractConsumeService extends AbstractService {

    protected final String topicInput;
    protected final KafkaReceiver<String, String> kafkaReceiver;
    protected final Duration delay; // How long does the pure processing take?

    protected AbstractConsumeService(
        ApplicationProperties applicationProperties,
        KafkaReceiver<String, String> kafkaReceiver,
        CounterService counterService
    ) {
        super(applicationProperties, counterService);
        this.topicInput = applicationProperties.getTopicB();
        this.kafkaReceiver = kafkaReceiver;
        this.delay = applicationProperties.getProcessing().getWaitTime();
        LOGGER.info("{}: acknowledgementQueueMode={}", getClass().getSimpleName(),
            applicationProperties.getConsumer().getAcknowledgementQueueMode());
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
        LOGGER.info("{}: Start reading from topic \"{}\"", getClass().getSimpleName(), topicInput);
        return kafkaReceiver.receiveManual(Collections.singleton(topicInput))
            // kafka consume retries
            .retryWhen(retryBackoffSpec)
            .doOnNext(this::logReceived);
    }

    /**
     * The consumer task, that may take some time (defined by APPLICATION_PROCESSING_TIME) for processing an input.
     */
    protected Mono<KafkaReceiverRecord<String, String>> process(KafkaReceiverRecord<String, String> inputRecord) {
        return Mono.delay(this.delay)
            .map(result -> coreProcess(inputRecord))
            .doOnError(throwable -> {
                LOGGER.error("Error processing record from topic \"{}\" with key={}",
                    inputRecord.topicPartition().topic(), inputRecord.key(), throwable);
                inputRecord.nacknowledge(throwable);
            })
            .doOnNext(this::logProcessed);
    }

    /**
     * The core consumer task without additional waiting time.
     * Here a simple convert toUpperCase and some logging.
     */
    protected KafkaReceiverRecord<String, String> coreProcess(KafkaReceiverRecord<String, String> receiverRecord) {
        final ConsumerRecord<String,String> consumerRecord = receiverRecord.consumerRecord();
        final String input = consumerRecord.value();
        // A real consumer would do some meaningful reactive work here, e.g. write the record to a database using R2DBC
        final String ignored = input.toUpperCase(Locale.ROOT);
        return receiverRecord;
    }

    /**
     * Commit the given record manually.
     * @param receiverRecord record to commit
     * @return empty Mono
     */
    protected Mono<Void> manualCommit(KafkaReceiverRecord<String, String> receiverRecord) {
        return Mono.fromRunnable(receiverRecord::acknowledge)
            .doOnSuccess(unused -> logCommited(receiverRecord))
            .doOnError(this::logCommitError)
            .then();
    }

    /**
     * Commit the given record manually. Identical to manualCommit, but for discarded records.
     * will provide a different log entry.
     * @param receiverRecord record to commit
     */
    protected void onDiscardCommit(KafkaReceiverRecord<String, String> receiverRecord) {
        receiverRecord.acknowledge();
        logDiscardCommited(receiverRecord);
    }
}