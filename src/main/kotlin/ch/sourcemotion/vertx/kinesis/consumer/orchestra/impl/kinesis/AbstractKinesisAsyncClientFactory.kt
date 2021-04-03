package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.KinesisClientOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.AwsClientMetricFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.AwsClientMetricOptions
import io.vertx.core.Vertx
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import java.net.URI

abstract class AbstractKinesisAsyncClientFactory(
    protected val vertx: Vertx,
    protected val region: String,
    protected val kinesisClientOptions: KinesisClientOptions,
    protected val awsClientMetricOptions: AwsClientMetricOptions? = null
) {
    protected fun baseBuilderOf(awsCredentialsProvider: AwsCredentialsProvider): KinesisAsyncClientBuilder {
        val builder = KinesisAsyncClient.builder()
            .region(Region.of(region))
            .credentialsProvider(awsCredentialsProvider)
            .overrideConfiguration { configurationBuilder ->
                AwsClientMetricFactory.create(vertx, awsClientMetricOptions)
                    ?.let { metricPublisher -> configurationBuilder.addMetricPublisher(metricPublisher) }
            }
        kinesisClientOptions.kinesisEndpoint?.let { builder.endpointOverride(URI(it)) }
        return builder
    }
}