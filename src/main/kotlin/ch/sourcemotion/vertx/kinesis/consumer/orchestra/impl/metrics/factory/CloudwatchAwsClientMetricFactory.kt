package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory

import io.vertx.core.Vertx
import software.amazon.awssdk.metrics.MetricPublisher
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher
import java.time.Duration

/**
 * Factory to create a {@link CloudWatchMetricPublisher} that periodically forwards
 * AWS client metrics to AWS Cloudwatch.
 */
class CloudwatchAwsClientMetricFactory : AwsClientMetricFactory<CloudwatchAwsClientMetricOptions>() {

    override fun createMetricsPublisher(vertx: Vertx, options: CloudwatchAwsClientMetricOptions?): MetricPublisher {
        val builder = CloudWatchMetricPublisher.builder()
        options?.namespace?.let { builder.namespace(it) }
        options?.uploadFrequency?.let { builder.uploadFrequency(it) }
        options?.maximumCallsPerUpload?.let { builder.maximumCallsPerUpload(it) }
        return builder.build()
    }
}

/**
 * Options on how to publish AWS SDK metrics.
 */
data class CloudwatchAwsClientMetricOptions(
        var namespace: String? = null,
        var uploadFrequency: Duration? = null,
        var maximumCallsPerUpload: Int? = null,
        override var factoryClassName: String? = CloudwatchAwsClientMetricFactory::class.qualifiedName,
        override var enabled: Boolean = true
) : AwsClientMetricOptions

