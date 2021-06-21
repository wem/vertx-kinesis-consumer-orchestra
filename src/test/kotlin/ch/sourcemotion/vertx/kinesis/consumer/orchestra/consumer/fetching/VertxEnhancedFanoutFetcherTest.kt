package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.EnhancedFanOutOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_CLUSTER_ORCHESTRA_NAME
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.shardIds
import org.junit.jupiter.api.Disabled
import software.amazon.awssdk.services.kinesis.model.StreamDescription

@Disabled("There are issues with the Http/2 configuration. Those must first get identified and solved")
internal class VertxEnhancedFanoutFetcherTest : AbstractEnhancedFanoutFetcherTest() {

    override suspend fun prepareSut(streamDescription: StreamDescription): EnhancedFanoutFetcher {
        val enhancedFanOutOptions = EnhancedFanOutOptions(streamDescription.streamARN())
        val fetcherOptions = FetcherOptions(enhancedFanOut = enhancedFanOutOptions)

        val shardId = streamDescription.shardIds().first()
        return EnhancedFanoutFetcher(fetcherOptions, enhancedFanOutOptions, TEST_CLUSTER_ORCHESTRA_NAME, null,
            defaultTestScope, shardId, kinesisClient, null)
    }
}