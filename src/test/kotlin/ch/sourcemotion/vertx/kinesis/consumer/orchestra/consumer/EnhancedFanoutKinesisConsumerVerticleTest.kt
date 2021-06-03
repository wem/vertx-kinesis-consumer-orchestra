package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.EnhancedFanOutOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.shareNettyKinesisAsyncClientFactory
import org.junit.jupiter.api.BeforeEach
import software.amazon.awssdk.services.kinesis.model.StreamDescription

internal class EnhancedFanoutKinesisConsumerVerticleTest : AbstractKinesisConsumerVerticleTest() {

    @BeforeEach
    internal fun setUpNettyKinesisClient() = asyncBeforeOrAfter {
        vertx.shareNettyKinesisAsyncClientFactory()
    }

    override fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions {
        val enhancedFanOutOptions = EnhancedFanOutOptions(streamDescription.streamARN())
        return FetcherOptions(enhancedFanOut = enhancedFanOutOptions)
    }
}
