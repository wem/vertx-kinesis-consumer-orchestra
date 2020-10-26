package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory

import io.vertx.core.Vertx
import software.amazon.awssdk.metrics.MetricPublisher

/**
 * Factory to disable AWS metric publishing.
 */
class DisabledAwsClientMetricFactory : AwsClientMetricFactory<DisabledAwsClientMetricOptions>() {

    override fun createMetricsPublisher(vertx: Vertx, options: DisabledAwsClientMetricOptions?): MetricPublisher {
        throw NotImplementedError()
    }
}

/**
 * Explicitly disable metric publishing
 */
data class DisabledAwsClientMetricOptions(
        override var enabled: Boolean = false,
        override var factoryClassName: String? = DisabledAwsClientMetricFactory::class.qualifiedName
) : AwsClientMetricOptions

