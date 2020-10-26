package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.vertx.core.Vertx
import mu.KLogging
import software.amazon.awssdk.metrics.MetricPublisher
import kotlin.reflect.full.createInstance

/**
 * Factory to provide a {@link MetricPublisher} that published AWS SDK client metrics.
 */
abstract class AwsClientMetricFactory<CONFIG : AwsClientMetricOptions> {

    abstract fun createMetricsPublisher(vertx: Vertx, options: CONFIG?): MetricPublisher

    companion object : KLogging() {
        fun create(vertx: Vertx, optionsClient: AwsClientMetricOptions?): MetricPublisher?  {
            if (optionsClient == null || !optionsClient.enabled) {
                return null
            }
            val className = optionsClient.factoryClassName
            val kClass = Class.forName(className).kotlin
            val instance = kClass.createInstance()
            if (instance is AwsClientMetricFactory<*>) {
                @Suppress("UNCHECKED_CAST")
                instance as AwsClientMetricFactory<AwsClientMetricOptions>
                val metricPublisher = instance.createMetricsPublisher(vertx, optionsClient)
                logger.info("Using metric publisher '${metricPublisher.javaClass.kotlin.qualifiedName}'")
                return metricPublisher
            } else {
                throw IllegalArgumentException("Class '$className' is not a '${AwsClientMetricFactory::class.qualifiedName}'.")
            }
        }
    }

}

/**
 * Options on how to publish AWS SDK metrics.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.CLASS,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonIgnoreProperties(ignoreUnknown = true)
interface AwsClientMetricOptions {
    /**
     * Full qualified class name of a {@link AwsMetricFactory} implementation.
     */
    var factoryClassName: String?
    var enabled: Boolean
}
