package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.EnhancedFanOutOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import io.micrometer.core.instrument.Counter
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


internal class EnhancedFanoutFetcher(
    fetcherOptions: FetcherOptions,
    private val enhancedOptions: EnhancedFanOutOptions,
    clusterName: OrchestraClusterName,
    startingSequenceNumber: SequenceNumber?,
    private val scope: CoroutineScope,
    private val shardId: ShardId,
    private val kinesis: KinesisAsyncClient,
    private val metricCounter: Counter?
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

    // Internal state, mainly used for testing to avoid race conditions
    var fetching = false
        private set
    private var subscriptionControl: SubscriptionControl? = null
    private var job: Job? = null

    override suspend fun start() {
        suspendCancellableCoroutine<Unit> { cont ->
            job = scope.launch {
                val consumer = runCatching {
                    getOrRegisterConsumer().also {
                        awaitConsumerActive()
                        cont.resume(Unit)
                        logger.info { "Enhanced fan out fetcher on stream \"${enhancedOptions.streamArn}\" and shard \"$shardId\" started" }
                    }
                }.getOrElse {
                    cont.resumeWithException(it)
                    return@launch
                }

                running = true

                while (running && scope.isActive) {
                    val subscribeRequest = subscribeToShardRequestOf(consumer)
                    runCatching {
                        throttleSubscription()
                        val job = subscribeToShard(subscribeRequest)
                        fetching = true
                        job.await()
                    }.onSuccess {
                        if (running) {
                            logger.info { "Enhanced fan out subscription done on stream \"$streamName\" and shard \"$shardId\". Will resubscribe" }
                        } else {
                            logger.info { "Enhanced fan out fetcher on stream \"$streamName\" and shard \"$shardId\" stopped" }
                        }
                    }.onFailure {
                        if (it is CancellationException
                            || (it is CompletionException && it.cause is CancellationException)
                        ) {
                            if (running) {
                                logger.info { "Enhanced fan out on stream \"$streamName\" and shard \"$shardId\" cancelled. Will resubscribe" }
                            } else {
                                logger.info { "Enhanced fan out on stream \"$streamName\" and shard \"$shardId\" cancelled. Fetcher stopped" }
                            }
                        } else {
                            when {
                                it is ResourceInUseException -> logger.info { "Enhanced fan out subscription did fail on stream \"$streamName\" and shard \"$shardId\", because consumer was not ready. Will resubscribe" }
                                running -> logger.warn(it) { "Enhanced fan out subscription did end exceptionally on stream \"$streamName\" and shard \"$shardId\". Will resubscribe" }
                                else -> logger.info { "Enhanced fan out subscription did end \"$streamName\" and shard \"$shardId\" as fetcher is no more running" }
                            }
                        }
                    }
                    cancelSubscription()
                }
            }
        }
    }

    override suspend fun stop() {
        running = false
        cancelSubscription()
        runCatching { job?.cancel("Enhanced fan out fetcher on stream \"$streamName\" and shard \"$shardId\" stopped") }
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
     * Executes the final subscription to a shard. Afterwards the [EventSubscriber.onNext] will get called for received
     * records.
     */
    private fun subscribeToShard(request: SubscribeToShardRequest): CompletableFuture<Void> {
        val responseHandler = SubscribeToShardResponseHandler.builder()
            .subscriber(Supplier {
                EventSubscriber(
                    scope, streamName, shardId, streamWriter, currentSequenceNumberRef, metricCounter
                ).also { this.subscriptionControl = it }
            })
            .build()

        return kinesis.subscribeToShard(request, responseHandler)
            .also { logger.debug { "Enhanced fan out did finally subscribe to stream \"$streamName\" and shard \"$shardId\"" } }
    }

    private suspend fun getOrRegisterConsumer(): Consumer {
        val existingConsumer =
            kinesis.getStreamConsumersQuery().consumers().firstOrNull { it.consumerName() == consumerName }

        val consumer = if (existingConsumer == null) {
            logger.debug { "Enhanced fan out consumer \"${consumerName}\" not exists on stream \"${streamArn}\". Will create it" }
            kinesis.registerStreamConsumerCmd().consumer()
        } else existingConsumer

        consumerArn = consumer.consumerARN()

        logger.debug { "Enhanced fan out fetcher will use consumer \"$consumerArn\" on stream \"$streamName\" and shard \"$shardId\"" }

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
        return SubscribeToShardRequest.builder()
            .consumerARN(consumer.consumerARN())
            .shardId("$shardId")
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
            }
            .build().also {
                val startingPosition = it.startingPosition()
                val startingPositionValue = startingPosition.typeAsString()
                val sequenceNumber = startingPosition.sequenceNumber()
                logger.info {
                    "Subscribe to shard request on stream \"$streamName\" and shard \"$shardId\" contains " +
                            "iterator type \"$startingPositionValue\" " +
                            "at sequence number \"$sequenceNumber\""
                }
            }
    }
}

private class EventSubscriber(
    private val scope: CoroutineScope,
    private val streamName: String,
    private val shardId: ShardId,
    private val streamWriter: RecordBatchStreamWriter,
    private val currentSequenceNumberRef: SequenceNumberRef,
    private val metricCounter: Counter?
) : Subscriber<SubscribeToShardEventStream>, SubscriptionControl {

    private companion object : KLogging()

    @Volatile
    private lateinit var subscription: Subscription

    private var finished = false

    override fun onSubscribe(s: Subscription) {
        this.subscription = s
        requestForNextEvent()
        logger.debug { "Event subscriber started on stream \"$streamName\" and shard \"$shardId\"." }
    }

    override fun onNext(event: SubscribeToShardEventStream) {
        if (finished) {
            return
        }
        if (event is SubscribeToShardEvent) {
            scope.launch {
                try {
                    val latestRecord = event.records().lastOrNull()
                    val continuationSequenceNumber = event.continuationSequenceNumber()
                    currentSequenceNumberRef.value = if (continuationSequenceNumber != null) {
                        if (latestRecord != null) {
                            if (latestRecord.sequenceNumber() == continuationSequenceNumber) {
                                SequenceNumber(continuationSequenceNumber, SequenceNumberIteratorPosition.AFTER)
                            } else {
                                SequenceNumber(continuationSequenceNumber, SequenceNumberIteratorPosition.AT)
                            }
                        } else {
                            SequenceNumber(continuationSequenceNumber, SequenceNumberIteratorPosition.AT)
                        }
                    } else {
                        null
                    }
                    streamWriter.writeToStream(event)
                    runCatching { metricCounter?.increment(event.records().size.toDouble()) }
                } catch (e: Exception) {
                    logger.warn(e) { "Unable to write event on stream \"$streamName\" and shard \"$shardId\" to record stream \"$event\"." }
                } finally {
                    requestForNextEvent()
                }
            }
        } else {
            logger.debug { "Received unprocessable event \"$event\" on stream \"$streamName\" and shard \"$shardId\"." }
            requestForNextEvent()
        }
    }

    private fun requestForNextEvent() {
        if (finished.not()) {
            subscription.request(1)
        }
    }

    override fun onError(t: Throwable) {
        if (finished.not()) {
            logger.warn(t) { "Error during streaming on stream \"$streamName\" and shard \"$shardId\"." }
        }
        cancel()
    }

    override fun onComplete() {
        cancel()
        logger.debug { "Subscription on stream \"$streamName\" and shard \"$shardId\" did end." }
    }

    override fun cancel() {
        if (finished.not()) {
            finish()
            runCatching { subscription.cancel() }
        }
    }

    private fun finish() {
        if (finished.not()) {
            logger.info { "Finish subscriber on stream \"$streamName\" and shard \"$shardId\"" }
            finished = true
        }
    }
}

interface SubscriptionControl {
    fun cancel()
}

private class SequenceNumberRef(var value: SequenceNumber? = null)