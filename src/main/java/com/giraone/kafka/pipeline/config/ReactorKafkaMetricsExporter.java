package com.giraone.kafka.pipeline.config;

import io.atleon.micrometer.AbstractKafkaMetricsReporter;
import org.apache.kafka.common.metrics.KafkaMetric;

/**
 * Replicates metric formats provided by legacy reactor-kafka implementation
 */
public class ReactorKafkaMetricsExporter extends AbstractKafkaMetricsReporter {

    @Override
    protected String extractMetricName(KafkaMetric metric) {
        return removeUpToLastAndIncluding(metric.metricName().name(), '.');
    }

    @Override
    protected String extractMeterNamePrefix(KafkaMetric metric) {
        return super.extractMeterNamePrefix(metric).replace("-metrics", "");
    }
}
