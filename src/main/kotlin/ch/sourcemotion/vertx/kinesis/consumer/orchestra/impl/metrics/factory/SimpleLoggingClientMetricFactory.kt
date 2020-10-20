package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.publisher.SimpleLoggingPublisher
import io.vertx.core.Vertx
import software.amazon.awssdk.metrics.MetricPublisher

/**
 * Factory to create a {@link CloudWatchMetricPublisher} that periodically forwards
 * AWS client metrics to AWS Cloudwatch.
 */
class SimpleLoggingClientMetricFactory : AwsClientMetricFactory<SimpleLoggingAwsClientMetricOptions>() {

    override fun createMetricsPublisher(vertx: Vertx, options: SimpleLoggingAwsClientMetricOptions?): MetricPublisher {
        return SimpleLoggingPublisher()
    }
}

/**
 * Options on how to log AWS SDK metrics.
 */
data class SimpleLoggingAwsClientMetricOptions(
        override var enabled: Boolean = true,
        override var factoryClassName: String? = SimpleLoggingClientMetricFactory::class.qualifiedName
) : AwsClientMetricOptions