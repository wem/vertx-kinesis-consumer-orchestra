package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.api.KinesisConsumerException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.awaitSuspending
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingEventFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardStatePersistence
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardStatePersistenceFactory
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.time.Duration
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Verticle which should be implemented to receive records polled from Kinesis.
 * Is configurable in orchestra options [ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions#consumerVerticleClass]
 */
abstract class AbstractKinesisConsumerVerticle : CoroutineVerticle() {

    companion object : KLogging() {
        internal const val CONSUMER_START_CMD_ADDR = "/kinesis-consumer-orchester/start-consumer"
    }

    private val options: KinesisConsumerVerticleOptions by lazy {
        config.mapTo(KinesisConsumerVerticleOptions::class.java)
    }

    protected lateinit var shardId: ShardId

    private val redis by lazy { Redis.createClient(vertx, options.redisOptions) }

    private val kinesisClient: KinesisAsyncClient by lazy {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    private val shardStatePersistence: ShardStatePersistence by lazy {
        SharedData.getSharedInstance<ShardStatePersistenceFactory>(vertx, ShardStatePersistenceFactory.SHARED_DATA_REF)
            .createShardStatePersistence(RedisAPI.api(redis))
    }

    private var keepAliveTimerId: TimerId? = null
    private var running = false

    override suspend fun start() {
        vertx.eventBus().localConsumer(CONSUMER_START_CMD_ADDR, this::onStartConsumerCmd)
        logger.info { "Kinesis consumer verticle ready to get started" }
    }

    override suspend fun stop() {
        running = false
        runCatching {
            removeShardProgressFlag()
        }.onSuccess { logger.info { "Kinesis consumer verticle stopped successfully on ${consumerInfo()}" } }
            .onFailure { logger.warn { "Unable to remove shard progress flag on ${consumerInfo()}" } }
        runCatching { shardStatePersistence.close() }.onFailure { logger.warn(it) { "Unable to close Redis client." } }
        runCatching { kinesisClient.close() }.onFailure { logger.warn(it) { "Unable to close Kinesis client." } }
    }

    /**
     * Consumer function which will be called when this verticle should start polling records from Kinesis.
     */
    private fun onStartConsumerCmd(msg: Message<String>) {
        shardId = msg.body().asShardIdTyped()

        logger.debug { "Try to start consumer on ${consumerInfo()}" }

        launch {
            keepAliveTimerId = shardStatePersistence.startShardProgressAndKeepAlive(vertx, this, shardId)

            val shardIterator = getStartShardIterator()
            shardStatePersistence.saveShardIterator(shardId, shardIterator)

            // Start the fun
            running = true
            startPolling(shardIterator)
        }.invokeOnCompletion { throwable ->
            throwable?.let { msg.fail(0, it.message) } ?: msg.reply(null)
        }
    }

    private fun startPolling(startShardIterator: ShardIterator) {
        logger.debug { "Start polling for ${consumerInfo()}" }
        var shardIteratorToQuery = startShardIterator
        launch {
            while (isActive && running) {
                runCatching {
                    kinesisClient.getRecords {
                        it.shardIterator(shardIteratorToQuery.iter)
                        it.limit(options.recordsPerPollLimit)
                    }.awaitSuspending()
                }.onSuccess { recordsResponse ->
                    // We throttle as coroutine, so when the record processing will take longer than the configured
                    // interval, we can continue polling immediately afterwards.
                    val throttleJob = launch {
                        delay(options.kinesisPollIntervalMillis)
                    }

                    val records = recordsResponse.records()
                    val nextIterator = recordsResponse.nextShardIterator()?.let { ShardIterator(it) }

                    runCatching {
                        deliver(records)
                        handleConsumerSuccess(nextIterator)
                    }.getOrElse { handleConsumerFailure(it, nextIterator, shardIteratorToQuery) }?.let {
                        shardIteratorToQuery = it
                        throttleJob.join()
                    } ?: onShardDidEnd()

                }.onFailure { throwable ->
                    logger.warn(throwable) { "Failure during polling records on ${consumerInfo()} ... but polling will be continued" }
                }
            }
        }.invokeOnCompletion {
            logger.debug { "Polling for ${consumerInfo()} did end. Shard did end=\"$running\"" }
        }
    }

    private suspend fun handleConsumerSuccess(nextIterator: ShardIterator?) =
        nextIterator?.let { shardStatePersistence.saveShardIterator(shardId, nextIterator) }

    private suspend fun handleConsumerFailure(
        throwable: Throwable,
        usuallyNextIterator: ShardIterator?,
        previousShardIterator: ShardIterator
    ): ShardIterator? {
        return if (throwable is KinesisConsumerException) {
            if (retryFromFailedRecord(throwable)) {
                val failedRecord = throwable.failedRecord
                return kinesisClient.getShardIteratorAtSequenceNumber(
                    options.streamName,
                    shardId,
                    failedRecord.sequenceNumber
                ).asShardIteratorTyped()
            }
            usuallyNextIterator
        } else {
            if (retryFromFailedRecord()) {
                logger.warn(throwable) {
                    "Kinesis consumer configured to retry from failed record, but no record " +
                            "information available as wrong exception type \"${throwable::class.java.name}\" was thrown. " +
                            "To retry consume, beginning from failed record you must throw an exception of type " +
                            "\"${KinesisConsumerException::class.java.name}\", so we retry from latest shard iterator"
                }
                return previousShardIterator
            }
            usuallyNextIterator
        }.also { shardIterator -> shardIterator?.let { shardStatePersistence.saveShardIterator(shardId, it) } }
    }

    private suspend fun onShardDidEnd() {
        logger.debug { "Streaming on ${consumerInfo()} ended because shard iterator did reach its end. Start resharding process..." }
        running = false
        val streamDesc = kinesisClient.streamDescriptionWhenActiveAwait(options.streamName)
        persistShardIsFinished(streamDesc)
        removeShardProgressFlag()
        shardStatePersistence.deleteShardIterator(shardId)

        val reshardingInfo = ReshardingEventFactory(
            streamDesc,
            options.streamName,
            shardId
        ).createReshardingEvent()

        vertx.eventBus().send(reshardingInfo.getNotificationAddr(), reshardingInfo, DeliveryOptions())
    }

    private suspend fun persistShardIsFinished(streamDesc: StreamDescription) {
        shardStatePersistence.saveFinishedShard(
            shardId,
            Duration.ofHours(streamDesc.retentionPeriodHours().toLong()).toMillis()
        )
        logger.debug { "Set ${consumerInfo()} as finished" }
    }

    private suspend fun removeShardProgressFlag() {
        keepAliveTimerId?.let { timerId -> vertx.cancelTimer(timerId.id) }
        shardStatePersistence.flagShardNoMoreInProgress(shardId)
        logger.debug { "Remove ${consumerInfo()} from in progress list" }
    }

    private suspend fun getStartShardIterator(): ShardIterator {
        return when (options.shardIteratorStrategy) {
            ShardIteratorStrategy.FORCE_LATEST -> {
                logger.debug { "Force ${ShardIteratorType.LATEST.name} shard iterator on ${consumerInfo()}" }
                kinesisClient.getLatestShardIterator(options.streamName, shardId).asShardIteratorTyped()
            }
            ShardIteratorStrategy.EXISTING_OR_LATEST -> {
                val existingIterator = shardStatePersistence.getShardIterator(shardId)
                if (existingIterator.isNotNull()) {
                    logger.debug { "Use existing shard iterator: \"$existingIterator\" for ${consumerInfo()} because no existing iterator found" }
                    existingIterator
                } else {
                    logger.debug { "Get ${ShardIteratorType.LATEST.name} shard iterator for ${consumerInfo()} because no existing iterator found" }
                    kinesisClient.getLatestShardIterator(options.streamName, shardId).asShardIteratorTyped()
                }
            }
        }
    }

    private suspend fun deliver(records: List<Record>) {
        suspendCancellableCoroutine<Unit> { cont ->
            onRecords(records, Handler {
                launch {
                    if (it.succeeded()) {
                        cont.resume(Unit)
                    } else {
                        cont.resumeWithException(it.cause())
                    }
                }
            })
        }
    }

    private fun retryFromFailedRecord(consumerException: KinesisConsumerException? = null) =
        consumerException?.errorHandling == ErrorHandling.RETRY_FROM_FAILED_RECORD
                || options.errorHandling == ErrorHandling.RETRY_FROM_FAILED_RECORD

    /**
     * Concrete consumer verticle implementations should / can implement final Kinesis record handling here.
     */
    protected abstract fun onRecords(records: List<Record>, handler: Handler<AsyncResult<Void>>)

    private fun consumerInfo() =
        "{ application: \"${options.applicationName}\", stream: \"${options.streamName}\", shard: \"${shardId}\", verticle: \"${this::class.java.name}\" }"
}
