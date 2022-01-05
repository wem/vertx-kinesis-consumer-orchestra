package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.shareKinesisAsyncClientFactory
import io.vertx.core.Vertx
import software.amazon.awssdk.services.kinesis.model.StreamDescription

internal class DynamicFetcherMultiNodeComponentTest : AbstractMultiNodeComponentTest() {

    override fun Vertx.setUpKinesisClient() {
        shareKinesisAsyncClientFactory()
    }

    override fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions = FetcherOptions()
}