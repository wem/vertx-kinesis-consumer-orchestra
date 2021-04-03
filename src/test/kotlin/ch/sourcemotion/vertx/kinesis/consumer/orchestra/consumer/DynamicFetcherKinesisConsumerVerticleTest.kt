package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import software.amazon.awssdk.services.kinesis.model.StreamDescription

internal class DynamicFetcherKinesisConsumerVerticleTest : AbstractKinesisConsumerVerticleTest() {
    override fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions = FetcherOptions()
}
