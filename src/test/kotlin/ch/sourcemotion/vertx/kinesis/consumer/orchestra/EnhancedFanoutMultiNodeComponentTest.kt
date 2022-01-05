package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.shareNettyKinesisAsyncClientFactory
import io.vertx.core.Vertx
import software.amazon.awssdk.services.kinesis.model.StreamDescription

internal class EnhancedFanoutMultiNodeComponentTest : AbstractMultiNodeComponentTest() {
    override fun Vertx.setUpKinesisClient() {
        shareNettyKinesisAsyncClientFactory()
    }

    override fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions {
        val enhancedFanOutOptions = EnhancedFanOutOptions(streamDescription.streamARN(), sdkNettyMaxConcurrency = 100)
        return FetcherOptions(enhancedFanOut = enhancedFanOutOptions)
    }
}