package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.publisher

import mu.KLogging
import software.amazon.awssdk.metrics.MetricCollection
import software.amazon.awssdk.metrics.MetricPublisher

class SimpleLoggingPublisher() : MetricPublisher, KLogging() {

    override fun publish(metricCollection: MetricCollection?) {
        logger.info { "Metrics: $metricCollection"}
    }

    override fun close() {
        logger.info { "Close" }
    }
}