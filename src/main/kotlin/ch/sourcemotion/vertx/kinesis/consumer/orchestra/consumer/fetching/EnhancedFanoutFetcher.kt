package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.EnhancedFanOutOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import io.micrometer.core.instrument.Counter
import io.vertx.core.Context
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
    private val context: Context,
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
                    vertx, context, scope, streamName, shardId, streamWriter, currentSequenceNumberRef, metricCounter
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
    private val vertx: Vertx,
    private val context: Context,
    private val scope: CoroutineScope,
    private val streamName: String,
    private val shardId: ShardId,
    private val streamWriter: RecordBatchStreamWriter,
    private val currentSequenceNumberRef: SequenceNumberRef,
    private val metricCounter: Counter?
) : Subscriber<SubscribeToShardEventStream>, SubscriptionControl {

    private companion object : KLogging()

    private lateinit var subscription: Subscription

    private var finished = false
    private var firstRecordReceived = false
    private var additionalRequestCheckTimerId: Long? = null

    override fun onSubscribe(s: Subscription) {
        context.runOnContext {
            this.subscription = s
            subscription.request(1)
            logger.debug { "Event subscriber started on stream \"$streamName\" and shard \"$shardId\". Await events" }
            // TODO check if really needed
            additionalRequestCheckTimerId = vertx.setPeriodic(500) {
                if (firstRecordReceived.not() && finished.not()) {
                    subscription.request(1)
                } else {
                    vertx.runCatching { cancelTimer(it) }
                    additionalRequestCheckTimerId = null
                }
            }
        }
    }

    override fun onNext(event: SubscribeToShardEventStream) {
        if (firstRecordReceived.not()) {
            logger.debug { "Begin to receive events on stream \"$streamName\" and shard \"$shardId\"." }
            firstRecordReceived = true
        }
        if (event is SubscribeToShardEvent) {

            scope.launch {
                runCatching { metricCounter?.increment(event.records().size.toDouble()) }

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
            }.invokeOnCompletion {
                if (it != null) {
                    logger.warn(it) { "Unable to write event on stream \"$streamName\" and shard \"$shardId\" to record stream \"$event\"." }
                }
                doRequestOnSubscription()
            }
        } else {
            logger.debug { "Received unprocessable event \"$event\" on stream \"$streamName\" and shard \"$shardId\"." }
            doRequestOnSubscription()
        }
    }

    private fun doRequestOnSubscription() {
        if (finished.not()) {
            subscription.request(1)
        }
    }

    override fun onError(t: Throwable) {
        if (finished.not()) {
            logger.warn(t) { "Error during streaming on stream \"$streamName\" and shard \"$shardId\"." }
        }
        finish()
    }

    override fun onComplete() {
        finish()
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
            finished = true
            cancelTimers()
        }
    }

    private fun cancelTimers() {
        additionalRequestCheckTimerId?.let { vertx.runCatching { cancelTimer(it) } }
        additionalRequestCheckTimerId = null
    }
}

interface SubscriptionControl {
    fun cancel()
}

private class SequenceNumberRef(var value: SequenceNumber? = null)