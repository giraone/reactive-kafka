package com.giraone.kafka.pipeline.service.produce;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.AbstractService;
import com.giraone.kafka.pipeline.service.CounterService;
import io.atleon.kafka.KafkaSender;
import io.atleon.kafka.KafkaSenderRecord;
import io.atleon.kafka.KafkaSenderResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractProduceService extends AbstractService {

    // One single thread is enough to generate numbers and System.currentTimeMillis() tupels
    protected static final Scheduler schedulerForGenerateNumbers = Schedulers.newSingle("generateNumberScheduler", false);
    protected static final Scheduler schedulerForKafkaProduce = Schedulers.newSingle("producerScheduler", false);

    protected final KafkaSender<String, String> kafkaSender;
    protected final String topicOutput;
    protected final int maxNumberOfEvents;
    protected final Duration interval;

    protected AbstractProduceService(
        ApplicationProperties applicationProperties,
        CounterService counterService,
        KafkaSender<String, String> kafkaSender
    ) {
        super(applicationProperties, counterService);
        this.kafkaSender = kafkaSender;
        this.maxNumberOfEvents = applicationProperties.getProducerVariables().getMaxNumberOfEvents();
        this.interval = applicationProperties.getProducerVariables().getInterval();
        this.topicOutput = applicationProperties.getTopicA();
        LOGGER.info("If activated, {} will produce {} events to topic \"{}\" using an interval of {} ms.",
            getClass().getSimpleName(), maxNumberOfEvents, topicOutput, interval.toMillis());
    }

    protected Flux<Tuple2<String, String>> source(Duration delay, int limit) {
        return sourceHot(delay, limit);
    }

    protected Flux<Tuple2<String, String>> sourceHot(Duration delay, int limit) {

        final AtomicInteger counter = new AtomicInteger((int) (System.currentTimeMillis() / 1000L));
        return Flux.range(0, limit)
            .delayElements(delay, schedulerForGenerateNumbers)
            .map(ignored -> counter.getAndIncrement())
            .map(nr -> Tuples.of(Long.toString(nr), buildContent()))
            .doOnNext(t -> counterService.logRateProduced());
    }

    protected Flux<Tuple2<String, String>> sourceCold(Duration delay, int limit) {

        final AtomicInteger counter = new AtomicInteger((int) (System.currentTimeMillis() / 1000L));
        return Flux.interval(delay, schedulerForGenerateNumbers)
            .take(limit)
            .map(ignored -> counter.getAndIncrement())
            .map(nr -> Tuples.of(Long.toString(nr), buildContent()))
            .doOnNext(t -> counterService.logRateProduced());
    }

    protected Mono<KafkaSenderResult<String>> send(KafkaSenderRecord<String, String, String> senderRecord) {

        return kafkaSender.send(senderRecord)
            .doOnNext(senderResult ->
                counterService.logRateSent(senderResult.recordMetadata().get().partition(), senderResult.recordMetadata().get().offset()));
    }

    private String buildContent() {
        return String.valueOf((char) (65 + System.currentTimeMillis() % 26)).repeat(10);
    }
}