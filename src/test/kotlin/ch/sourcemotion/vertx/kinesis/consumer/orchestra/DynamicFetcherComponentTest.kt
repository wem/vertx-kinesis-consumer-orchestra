package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import software.amazon.awssdk.services.kinesis.model.StreamDescription

internal class DynamicFetcherComponentTest : AbstractComponentTest() {
    override fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions = FetcherOptions()
}