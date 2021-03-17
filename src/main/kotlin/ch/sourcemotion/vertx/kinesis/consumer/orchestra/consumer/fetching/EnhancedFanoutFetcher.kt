package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import mu.KLogging
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream


/**
 * TODO: Evaluate how to identify resharding situations. On pull approach an null next shard iterator is the signal, but what happen on enhanced fan out?
 */
internal class EnhancedFanoutFetcher(
    fetcherOptions: FetcherOptions,
) : Fetcher {
    private companion object : KLogging()

    private val recordBatchStream = RecordBatchStream(fetcherOptions.recordsPreFetchLimit)
    override val streamReader = recordBatchStream.reader()

    override suspend fun start() {
    }

    override fun resetTo(fetchPosition: FetchPosition) {
    }

    override suspend fun stop() {
    }
}

private class EventSubscriber(
) : Subscriber<SubscribeToShardEventStream>, SubscriptionControl {

    private companion object : KLogging()

    private lateinit var subscription: Subscription

    override fun onSubscribe(subscription: Subscription) {
        this.subscription = subscription
    }

    override fun onNext(event: SubscribeToShardEventStream) {
    }

    override fun onError(t: Throwable) {
    }

    override fun onComplete() {
    }

    override fun cancel() {
    }
}

interface SubscriptionControl {
    fun cancel()
}

private class SequenceNumberRef(var value: SequenceNumber? = null)