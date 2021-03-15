package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.EnhancedFanOutOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import io.vertx.core.Vertx
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import mu.KLogging
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.function.Supplier
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException


/**
 * TODO: Evaluate how to identify resharding situations. On pull approach an null next shard iterator is the signal, but what happen on enhanced fan out?
 */
internal class EnhancedFanoutFetcher(
    private val vertx: Vertx,
    fetcherOptions: FetcherOptions,
    private val enhancedOptions: EnhancedFanOutOptions,
    clusterName: OrchestraClusterName,
    startingSequenceNumber: SequenceNumber?,
    private val scope: CoroutineScope,
    private val shardId: ShardId,
    private val kinesis: KinesisAsyncClient
) : Fetcher {
    private companion object : KLogging()

    private val streamArn = enhancedOptions.streamArn
    private val streamName = clusterName.streamName
    private val consumerName = clusterName.applicationName
    private lateinit var consumerArn: String

    private val recordBatchStream = RecordBatchStream(fetcherOptions.recordsPreFetchLimit)
    private val streamWriter = recordBatchStream.writer()
    override val streamReader = recordBatchStream.reader()

    private val currentSequenceNumberRef = SequenceNumberRef(startingSequenceNumber)
    private var lastSubscriptionTimestamp: Long? = null
    private var running = false
    private var subscriptionControl: SubscriptionControl? = null

    override suspend fun start() {
        logger.info { "Start enhanced fan out fetcher on stream \"${enhancedOptions.streamArn}\" and shard \"$shardId\"" }
        suspendCancellableCoroutine<Unit> { cont ->
            scope.launch {
                val consumer = runCatching {
                    getOrRegisterConsumer().also {
                        cont.resume(Unit)
                        logger.info { "Enhanced fan out fetcher on stream \"${enhancedOptions.streamArn}\" and shard \"$shardId\" started" }
                    }
                }.getOrElse {
                    cont.resumeWithException(it)
                    return@launch
                }

                running = true

                while (running) {
                    val subscribeRequest = subscribeToShardRequestOf(consumer)
                    runCatching {
                        throttleSubscription()
                        val job = subscribeToShard(subscribeRequest)
                        job.await()
                    }.onSuccess {
                        cancelSubscription()
                        logger.info { "Enhanced fan out subscription done on stream \"$streamName\" and shard \"$shardId\". Will resubscribe" }
                    }.onFailure {
                        cancelSubscription()
                        if (it is CancellationException
                            || (it is CompletionException && it.cause is CancellationException)
                        ) {
                            if (running) {
                                logger.info { "Enhanced fan out on stream \"$streamName\" and shard \"$shardId\" cancelled. Will resubscribe" }
                            } else {
                                logger.info { "Enhanced fan out on stream \"$streamName\" and shard \"$shardId\" cancelled. Fetcher stopped" }
                            }
                        } else {
                            if (it is ResourceInUseException) {
                                logger.info { "Enhanced fan out subscription did fail on stream \"$streamName\" and shard \"$shardId\", because consumer was not ready. Will resubscribe" }
                            } else {
                                logger.warn(it) { "Enhanced fan out subscription did end exceptionally on stream \"$streamName\" and shard \"$shardId\". Will resubscribe" }
                            }
                        }
                    }
                }
            }
        }
    }

    override fun resetTo(fetchPosition: FetchPosition) {
        streamWriter.resetStream()
        currentSequenceNumberRef.value = fetchPosition.sequenceNumber
        cancelSubscription()
        logger.info { "Record fetcher reset on stream \"$streamName\" / shard \"$shardId\"" }
    }

    override suspend fun stop() {
        running = false
        cancelSubscription()
    }

    private fun cancelSubscription() {
        runCatching { subscriptionControl?.cancel() }
        subscriptionControl = null
    }

    /**
     * Enhanced fan out subscription can only happen each 5 second per consumer per shard.
     * So we have to wait for next if we try to subscribe too often.
     *
     * @see software.amazon.awssdk.services.kinesis.KinesisAsyncClient.subscribeToShard(software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest, software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler)
     */
    private suspend fun throttleSubscription() {
        val now = System.currentTimeMillis()
        lastSubscriptionTimestamp?.let {
            val millisSinceLastSubscription = now - it
            val millisToWait = enhancedOptions.minResubscribeIntervalMillis - millisSinceLastSubscription
            if (millisToWait > 0) {
                logger.info { "Wait $millisToWait for next subscription on stream \"$streamName\" and shard \"$shardId\"" }
                delay(millisToWait)
            }
        }
        lastSubscriptionTimestamp = now
    }

    /**
     * Called on pushed events from Kinesis
     */
    private suspend fun subscribeToShard(request: SubscribeToShardRequest): CompletableFuture<Void> {
        awaitConsumerActive()

        val responseHandler = SubscribeToShardResponseHandler.builder()
            .onError { cause -> logger.warn(cause) { "Enhanced fan out subscription on stream \"$streamName\" and shard \"$shardId\" failed" } }
            .subscriber(Supplier {
                EventSubscriber(
                    vertx, scope, streamName, shardId, streamWriter, currentSequenceNumberRef,
                    enhancedOptions.noEventReceivedThresholdMillis, ::cancelSubscription
                ).also { this.subscriptionControl = it }
            })
            .build()

        return kinesis.subscribeToShard(request, responseHandler)
            .also { logger.debug { "Enhanced fan out subscribed to stream \"$streamName\" and shard \"$shardId\"" } }
    }

    private suspend fun getOrRegisterConsumer(): Consumer {
        val existingConsumer =
            kinesis.getStreamConsumersQuery().consumers().firstOrNull { it.consumerName() == consumerName }

        val consumer = if (existingConsumer == null) {
            logger.info { "Enhanced fan out consumer \"${consumerName}\" not exists on stream \"${streamArn}\". Will create it" }
            kinesis.registerStreamConsumerCmd().consumer()
        } else existingConsumer

        consumerArn = consumer.consumerARN()

        logger.info { "Enhanced fan out fetcher will use consumer \"$consumerArn\" on stream \"$streamName\" and shard \"$shardId\"" }

        return consumer
    }

    private suspend fun KinesisAsyncClient.getStreamConsumersQuery(): ListStreamConsumersResponse {
        return listStreamConsumers { it.streamARN(streamArn) }.runCatching { await() }.getOrElse {
            if (it is LimitExceededException || it is ResourceInUseException) {
                delay(enhancedOptions.consumerRegistrationRetryIntervalMillis)
                getStreamConsumersQuery()
            } else {
                throw VertxKinesisConsumerOrchestraException(
                    "Unable to list enhanced fan out consumers on stream \"$streamArn\"", it
                )
            }
        }
    }

    private suspend fun KinesisAsyncClient.registerStreamConsumerCmd(): RegisterStreamConsumerResponse {
        return registerStreamConsumer { it.streamARN(streamArn).consumerName(consumerName) }.runCatching { await() }
            .getOrElse {
                if (it is LimitExceededException || it is ResourceInUseException) {
                    delay(enhancedOptions.consumerRegistrationRetryIntervalMillis)
                    registerStreamConsumerCmd()
                } else {
                    throw VertxKinesisConsumerOrchestraException(
                        "Unable to register enhanced fan out consumer \"$consumerName\" on stream \"$streamArn\" and shard \"$shardId\"",
                        it
                    )
                }
            }
    }

    /**
     * Awaits until the consumer becomes active.
     */
    private suspend fun awaitConsumerActive() {
        var consumerIsActive: Boolean
        do {
            val consumerActiveCheckInterval = enhancedOptions.consumerActiveCheckInterval
            consumerIsActive = kinesis.describeStreamConsumer {
                it.consumerARN(consumerArn).consumerName(consumerName).streamARN(streamArn)
            }.runCatching { await().consumerDescription().consumerStatus() == ConsumerStatus.ACTIVE }
                .getOrElse {
                    logger.warn(it) { "Enhanced fan out active check request failed for consumer on stream \"$streamName\" and shard \"$shardId\". Will check again in $consumerActiveCheckInterval millis" }
                    false
                }
            if (consumerIsActive.not()) {
                logger.debug { "Enhanced fan out consumer on stream \"$streamName\" and shard \"$shardId\" not ready (active). Will check again in $consumerActiveCheckInterval millis" }
                delay(consumerActiveCheckInterval)
            }
        } while (consumerIsActive.not())
    }

    private fun subscribeToShardRequestOf(consumer: Consumer): SubscribeToShardRequest {
        return SubscribeToShardRequest.builder().consumerARN(consumer.consumerARN()).shardId("$shardId")
            .startingPosition {
                val sequenceNumberToSubscribe = currentSequenceNumberRef.value
                if (sequenceNumberToSubscribe != null) {
                    it.sequenceNumber(sequenceNumberToSubscribe.number)
                    if (sequenceNumberToSubscribe.iteratorPosition == SequenceNumberIteratorPosition.AFTER) {
                        it.type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    } else {
                        it.type(ShardIteratorType.AT_SEQUENCE_NUMBER)
                    }
                } else {
                    it.type(ShardIteratorType.LATEST)
                }
            }.build()
    }
}

private class EventSubscriber(
    private val vertx: Vertx,
    private val scope: CoroutineScope,
    private val streamName: String,
    private val shardId: ShardId,
    private val streamWriter: RecordBatchStreamWriter,
    private val currentSequenceNumberRef: SequenceNumberRef,
    private val noEventReceivedThresholdMillis: Long,
    private val stop: () -> Unit
) : Subscriber<SubscribeToShardEventStream>, SubscriptionControl {

    private companion object : KLogging()

    private lateinit var subscription: Subscription

    private var canceled = false
    private var noEventsReceivedTimerId: Long? = null
    private var latestResponseTimestamp: Long = 0

    override fun onSubscribe(s: Subscription) {
        this.subscription = s
        subscription.request(1)
        logger.info { "Event subscriber started on stream \"$streamName\" and shard \"$shardId\". Await events" }
        noEventsReceivedTimerId = vertx.setPeriodic(noEventReceivedThresholdMillis) {
            val now = System.currentTimeMillis()
            val millisSinceLatestEvents = now - latestResponseTimestamp
            logger.debug { "No events received checker -> Latest events received \"$millisSinceLatestEvents\" millis ago on stream \"$streamName\" and shard \"$shardId\"" }
            if (millisSinceLatestEvents > noEventReceivedThresholdMillis) {
                logger.info { "Try to renew subscription on stream \"$streamName\" and shard \"$shardId\", because no events received within \"$noEventReceivedThresholdMillis\" millis" }
                stop()
            }
        }
    }

    override fun onNext(event: SubscribeToShardEventStream) {
        if (latestResponseTimestamp == 0L) {
            logger.info { "Begin to receive events on stream \"$streamName\" and shard \"$shardId\"" }
        }
        latestResponseTimestamp = System.currentTimeMillis()
        if (event is SubscribeToShardEvent) {
            currentSequenceNumberRef.value =
                SequenceNumber(event.continuationSequenceNumber(), SequenceNumberIteratorPosition.AFTER)

            scope.launch { streamWriter.writeToStream(event) }.invokeOnCompletion {
                if (it != null) {
                    logger.warn(it) { "Unable to write event on stream \"$streamName\" and shard \"$shardId\" to record stream \"$event\"" }
                }
                subscription.request(1)
            }
        } else {
            logger.debug { "Received unprocessable event \"$event\" on stream \"$streamName\" and shard \"$shardId\"" }
        }
    }

    override fun onError(t: Throwable) {
        logger.warn(t) { "Error during streaming on stream \"$streamName\" and shard \"$shardId\". Will cancel subscription" }
        stop()
    }

    override fun onComplete() {
        logger.info { "This subscription on stream \"$streamName\" and shard \"$shardId\" did end" }
        stop()
    }

    override fun cancel() {
        cancelNoEventsReceivedCheckTimer()
        if (canceled.not()) {
            canceled = true
            subscription.runCatching { cancel() }
        }
    }

    private fun cancelNoEventsReceivedCheckTimer() {
        noEventsReceivedTimerId?.let { runCatching { vertx.cancelTimer(it) } }
        noEventsReceivedTimerId = null
    }
}

interface SubscriptionControl {
    fun cancel()
}

private class SequenceNumberRef(var value: SequenceNumber? = null)