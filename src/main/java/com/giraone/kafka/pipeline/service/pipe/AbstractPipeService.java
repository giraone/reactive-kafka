package com.giraone.kafka.pipeline.service.pipe;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractService;
import com.giraone.kafka.pipeline.service.CounterService;
import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaReceiverRecord;
import io.atleon.kafka.KafkaSender;
import io.atleon.kafka.KafkaSenderRecord;
import io.atleon.kafka.KafkaSenderResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;

public abstract class AbstractPipeService extends AbstractService {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractPipeService.class);

    private final KafkaSender<String, String> kafkaSender;
    private final KafkaReceiver<String, String> kafkaReceiver;
    protected final String topicInput;
    protected final String topicOutput;
    protected final Duration delay; // How long does the pure processing take?
    protected final Retry retry;
    protected final Scheduler scheduler;

    protected AbstractPipeService(ApplicationProperties applicationProperties,
                                  CounterService counterService,
                                  KafkaSender<String, String> kafkaSender,
                                  KafkaReceiver<String, String> kafkaReceiver
    ) {
        super(applicationProperties, counterService);
        this.kafkaSender = kafkaSender;
        this.kafkaReceiver = kafkaReceiver;
        this.topicInput = applicationProperties.getTopicA();
        this.topicOutput = applicationProperties.getTopicB();
        this.delay = applicationProperties.getProcessing().getWaitTime();
        this.retry = applicationProperties.getConsumer().getRetrySpecification().toRetry();
        this.scheduler = applicationProperties.getProcessing().buildScheduler();
    }

    protected String getTopicInput() {
        return topicInput;
    }

    protected String getTopicOutput() {
        return topicOutput;
    }

    /**
     * The pipeline task, that may take some time (defined by APPLICATION_PROCESSING_TIME) for processing an input.
     */
    protected Mono<KafkaSenderRecord<String, String, KafkaReceiverRecord<String, String>>> process(KafkaReceiverRecord<String, String> inputRecord) {
        return Mono.delay(this.delay)
            .map(ignored -> coreProcess(inputRecord.value()))
            // pass KafkaReceiverRecord as correlation metadata to KafkaSenderRecord to be able to commit later
            .map(outputValue -> KafkaSenderRecord.create(getTopicOutput(), inputRecord.key(), outputValue, inputRecord));
    }

    /**
     * The core pipeline task, without the event metadata (message key) and without additional waiting time.
     * Here a simple convert toUpperCase.
     */
    protected String coreProcess(String input) {
        counterService.logRateProcessed();
        return input.toUpperCase(Locale.ROOT);
    }

    protected Flux<KafkaReceiverRecord<String, String>> receive() {
        final RetryBackoffSpec retryBackoffSpec = applicationProperties.getConsumer().getRetrySpecification().toRetry();
        return kafkaReceiver.receiveManual(Collections.singleton(topicInput))
            // kafka consume retries
            .retryWhen(retryBackoffSpec)
            .doOnNext(this::logReceived);
    }

    protected Mono<KafkaSenderResult<KafkaReceiverRecord<String, String>>> send(KafkaSenderRecord<String, String, KafkaReceiverRecord<String, String>> recordToSend) {
        return kafkaSender.send(recordToSend)
            .doOnNext(this::logSent);
    }

    protected Mono<Void> manualCommit(KafkaSenderResult<KafkaReceiverRecord<String, String>> senderResult) {
        return Mono.fromRunnable(() -> senderResult.correlationMetadata().acknowledge()) // commit vs. acknowledge
            .doOnSuccess(unused -> logCommited(senderResult.correlationMetadata()))
            .doOnError(this::logCommitError).then();
    }
}
