package com.giraone.kafka.pipeline.service.pipe;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractService;
import com.giraone.kafka.pipeline.service.CounterService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.Locale;

public abstract class AbstractPipeService extends AbstractService {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractPipeService.class);

    private final ReactiveKafkaProducerTemplate<String, String> kafkaSender;
    private final ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver;
    protected final String topicOutput;
    protected final Duration delay; // How long does the pure processing take?
    protected final Retry retry;
    protected final Scheduler scheduler;

    protected AbstractPipeService(ApplicationProperties applicationProperties,
                                  CounterService counterService,
                                  ReactiveKafkaProducerTemplate<String, String> kafkaSender,
                                  ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver
    ) {
        super(applicationProperties, counterService);
        this.kafkaSender = kafkaSender;
        this.kafkaReceiver = kafkaReceiver;
        this.topicOutput = applicationProperties.getTopicB();
        this.delay = applicationProperties.getProcessing().getWaitTime();
        this.retry = applicationProperties.getConsumer().getRetrySpecification().toRetry();
        this.scheduler = applicationProperties.getProcessing().buildScheduler();
    }

    protected String getTopicOutput() {
        return topicOutput;
    }

    /**
     * The pipeline task, that may take some time (defined by APPLICATION_PROCESSING_TIME) for processing an input.
     */
    protected Mono<SenderRecord<String, String, ReceiverRecord<String, String>>> process(ReceiverRecord<String, String> inputRecord) {
        return Mono.delay(this.delay)
            .map(ignored -> coreProcess(inputRecord.value()))
            // pass KafkaReceiverRecord as correlation metadata to KafkaSenderRecord to be able to commit later
            .map(outputValue -> SenderRecord.create(new ProducerRecord<>(getTopicOutput(), inputRecord.key(), outputValue), inputRecord));
    }

    /**
     * The core pipeline task, without the event metadata (message key) and without additional waiting time.
     * Here a simple convert toUpperCase.
     */
    protected String coreProcess(String input) {
        counterService.logRateProcessed();
        return input.toUpperCase(Locale.ROOT);
    }

    protected Flux<ReceiverRecord<String, String>> receive() {
        final RetryBackoffSpec retryBackoffSpec = applicationProperties.getConsumer().getRetrySpecification().toRetry();
        return kafkaReceiver.receive()
            // kafka consume retries
            .retryWhen(retryBackoffSpec)
            .doOnNext(this::logReceived);
    }

    protected Mono<SenderResult<ReceiverRecord<String, String>>> send(SenderRecord<String, String, ReceiverRecord<String, String>> recordToSend) {
        return kafkaSender.send(recordToSend)
            .doOnNext(this::logSent);
    }

    protected Mono<Void> manualCommit(SenderResult<ReceiverRecord<String, String>> senderResult) {
        return senderResult.correlationMetadata().receiverOffset().commit() // commit vs. acknowledge
            .doOnSuccess(unused -> logCommited(senderResult.correlationMetadata()))
            .doOnError(this::logCommitError).then();
    }
}
