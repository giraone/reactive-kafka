package com.giraone.kafka.pipeline.service.pipe;

import com.giraone.kafka.pipeline.config.ApplicationProperties;
import com.giraone.kafka.pipeline.service.CounterService;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

@Service
public class PipeReceiveSendService extends AbstractPipeService {

    public PipeReceiveSendService(
        ApplicationProperties applicationProperties,
        CounterService counterService,
        ReactiveKafkaProducerTemplate<String, String> kafkaSender,
        ReactiveKafkaConsumerTemplate<String, String> kafkaReceiver
    ) {
        super(applicationProperties, counterService, kafkaSender, kafkaReceiver);
    }

    //------------------------------------------------------------------------------------------------------------------

    @Override
    public void start() {

        LOGGER.info("Assembly of {}", this.getClass().getSimpleName());
        subscription = this.receive()
            // perform processing on another scheduler
            .publishOn(scheduler)
            // perform the pipe task
            .concatMap(this::process)
            // send result to target topic
            .concatMap(this::send)
            // commit every processed record in strict order
            .concatMap(this::manualCommit)
            // log any error
            .doOnError(e -> counterService.logError("PipeReceiveSendService failed!", e))
            // subscription main loop - restart on unhandled errors
            .subscribe(null, this::restartMainLoopOnError);
        counterService.logMainLoopStarted(getClass().getSimpleName());
    }

    @Override
    @EventListener
    public void onApplicationCloseEvent(ContextClosedEvent contextClosedEvent) {
        super.onApplicationCloseEvent(contextClosedEvent);
    }
}