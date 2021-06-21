package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.EnhancedFanOutOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.NettyKinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AWS_REGION
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_CLUSTER_ORCHESTRA_NAME
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.shardIds
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import kotlin.LazyThreadSafetyMode.NONE

/**
 * Tests defined in [AbstractEnhancedFanoutFetcherTest]
 */
internal class NettyEnhancedFanoutFetcherTest : AbstractEnhancedFanoutFetcherTest() {

    private val clientFactory by lazy(NONE) {
        NettyKinesisAsyncClientFactory(vertx, AWS_REGION)
    }

    override suspend fun prepareSut(streamDescription: StreamDescription): EnhancedFanoutFetcher {
        val enhancedFanOutOptions = EnhancedFanOutOptions(streamDescription.streamARN())
        val fetcherOptions = FetcherOptions(enhancedFanOut = enhancedFanOutOptions)

        val shardId = streamDescription.shardIds().first()
        return EnhancedFanoutFetcher(
            fetcherOptions, enhancedFanOutOptions, TEST_CLUSTER_ORCHESTRA_NAME, null,
            defaultTestScope, shardId, clientFactory.createKinesisAsyncClient(context), null
        )
    }
}