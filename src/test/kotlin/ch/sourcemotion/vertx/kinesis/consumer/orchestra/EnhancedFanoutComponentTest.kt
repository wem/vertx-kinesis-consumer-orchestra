package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.shareNettyKinesisAsyncClientFactory
import org.junit.jupiter.api.BeforeEach
import software.amazon.awssdk.services.kinesis.model.StreamDescription

internal class EnhancedFanoutComponentTest : AbstractComponentTest() {
    @BeforeEach
    internal fun setUpNettyKinesisClient() = asyncBeforeOrAfter {
        vertx.shareNettyKinesisAsyncClientFactory(getKinesisEndpointOverride())
    }

    override fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions {
        val enhancedFanOutOptions = EnhancedFanOutOptions(streamDescription.streamARN())
        return FetcherOptions(enhancedFanOut = enhancedFanOutOptions)
    }
}