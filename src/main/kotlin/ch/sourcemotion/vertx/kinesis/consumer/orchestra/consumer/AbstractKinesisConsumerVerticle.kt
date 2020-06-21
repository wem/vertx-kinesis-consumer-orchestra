package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingEventFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardStatePersistence
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardStatePersistenceFactory
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import kotlinx.coroutines.*
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.time.Duration
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Verticle which must be implemented to receive records polled from Kinesis.
 *
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
    private val recordFetcher: RecordFetcher by lazy {
        RecordFetcher(kinesisClient, options.recordsPerPollLimit, options.streamName, shardId) { consumerInfo }
    }

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

    /**
     * Running flag, if the verticle is still running. When the verticle  get stopped, this flag must be false.
     * Volatile because this flag is queried within coroutine, but set on "direct" event loop thread.
     */
    @Volatile
    private var running = false
    private var pollingJob: Job? = null

    override suspend fun start() {
        vertx.eventBus().localConsumer(CONSUMER_START_CMD_ADDR, this::onStartConsumerCmd)
        logger.debug { "Kinesis consumer verticle ready to get started" }
    }

    override suspend fun stop() {
        running = false
        suspendCancellableCoroutine<Unit> { cont ->
            pollingJob?.invokeOnCompletion {
                if (it.isNotNull()) {
                    logger.warn(it) { "\"$consumerInfo\" stopped exceptionally" }
                } else {
                    logger.debug { "\"$consumerInfo\" stopped" }
                }
                cont.resume(Unit)
            }
        }
        runCatching {
            removeShardProgressFlag()
        }.onSuccess { logger.info { "Kinesis consumer verticle stopped successfully on $consumerInfo" } }
            .onFailure { logger.warn { "Unable to remove shard progress flag on $consumerInfo" } }
        runCatching { shardStatePersistence.close() }.onFailure { logger.warn(it) { "Unable to close Redis client." } }
        runCatching { kinesisClient.close() }.onFailure { logger.warn(it) { "Unable to close Kinesis client." } }
    }

    /**
     * Consumer function which will be called when this verticle should start polling records from Kinesis.
     */
    private fun onStartConsumerCmd(msg: Message<String>) {
        shardId = msg.body().asShardIdTyped()

        logger.debug { "Try to start consumer on $consumerInfo" }

        launch {
            keepAliveTimerId = shardStatePersistence.startShardProgressAndKeepAlive(vertx, this, shardId)

            val startPosition = getQueryStartPosition()

            running = true

            startPolling(startPosition)
        }.invokeOnCompletion { throwable ->
            throwable?.let {
                logger.error(it) { "Unable to start Kinesis consumer verticle on \"$consumerInfo\"" }
                msg.fail(0, it.message)
            } ?: msg.reply(null)
        }
    }

    private fun startPolling(fetchStartPosition: QueryPosition) {
        logger.debug { "Start polling for $consumerInfo" }
        var positionToFetch = fetchStartPosition
        pollingJob = launch {
            while (isActive && running) {
                runCatching {
                    recordFetcher.fetchNextRecords(positionToFetch)
                }.onSuccess { recordsResponse ->
                    // We throttle as coroutine, so when the record processing will take longer than the configured
                    // interval, we can continue polling immediately afterwards.
                    val throttleJob = launch {
                        delay(options.kinesisPollIntervalMillis)
                    }

                    val records = recordsResponse.records()
                    // Can be null if the shard did end
                    val nextIterator = recordsResponse.nextShardIterator()?.asShardIteratorTyped()

                    // If the list of queried records was empty, we still use the previous sequence number, as it didn't change
                    val nextSequenceNumber = records.lastOrNull()?.sequenceNumber()?.asSequenceNumberAfter()
                        ?: positionToFetch.sequenceNumber

                    val nextPositionIfDeliveredSuccessful = nextIterator?.let { QueryPosition(it, nextSequenceNumber) }

                    val nextPositionToFetch = runCatching {
                        deliver(records)
                        nextPositionIfDeliveredSuccessful
                    }.getOrElse { throwable ->
                        handleConsumerFailure(throwable, nextPositionIfDeliveredSuccessful, positionToFetch)
                    }

                    if (nextPositionToFetch.isNotNull()) {
                        if (nextPositionToFetch != positionToFetch) {
                            // Sequence number could be null here, as if the initial delivery of records did fail and
                            // the iterator was based just on latest, there is no sequence number available
                            nextPositionToFetch.sequenceNumber?.let { sequenceNumber ->
                                shardStatePersistence.saveConsumerShardSequenceNumber(
                                    shardId,
                                    sequenceNumber
                                )
                            }
                            positionToFetch = nextPositionToFetch
                        }
                    } else {
                        onShardDidEnd()
                    }
                }.onFailure { throwable ->
                    logger.warn(throwable) { "Failure during polling records on $consumerInfo ... but polling will be continued" }
                }
            }
        }
    }

    private suspend fun handleConsumerFailure(
        throwable: Throwable,
        nextPosition: QueryPosition?,
        previousPosition: QueryPosition
    ): QueryPosition? {
        return if (retryFromFailedRecord(throwable)) {
            if (throwable is KinesisConsumerException) {
                val failedSequence = throwable.failedRecord.sequenceNumber.asSequenceNumberAt()
                val iteratorAtFailedSequence = kinesisClient.getShardIteratorBySequenceNumber(
                    options.streamName,
                    shardId,
                    failedSequence
                )
                logger.debug {
                    "Record processing did fail on \"$consumerInfo\". All information available to retry" +
                            "from failed record."
                }
                QueryPosition(iteratorAtFailedSequence, failedSequence)
            } else {
                logger.warn(throwable) {
                    "Kinesis consumer configured to retry from failed record, but no record " +
                            "information available as wrong exception type \"${throwable::class.java.name}\" was thrown. " +
                            "To retry consume, beginning from failed record you must throw an exception of type " +
                            "\"${KinesisConsumerException::class.java.name}\", so we retry from latest shard iterator"
                }
                previousPosition
            }
        } else {
            nextPosition
        }
    }

    private suspend fun onShardDidEnd() {
        logger.debug { "Streaming on $consumerInfo ended because shard iterator did reach its end. Start resharding process..." }
        running = false
        val streamDesc = kinesisClient.streamDescriptionWhenActiveAwait(options.streamName)
        persistShardIsFinished(streamDesc)
        removeShardProgressFlag()
        shardStatePersistence.deleteShardSequenceNumber(shardId)

        val reshardingInfo = ReshardingEventFactory(
            streamDesc,
            options.streamName,
            shardId
        ).createReshardingEvent()

        vertx.eventBus().send(reshardingInfo.getNotificationAddr(), reshardingInfo)
    }

    private suspend fun persistShardIsFinished(streamDesc: StreamDescription) {
        shardStatePersistence.saveFinishedShard(
            shardId,
            // The expiration of the shard finished flag, will be an hour after the shard retention.
            // So it's ensured that we not lose the finished flag of this shard and avoid death data.
            Duration.ofHours(streamDesc.retentionPeriodHours().toLong() + 1).toMillis()
        )
        logger.debug { "Set $consumerInfo as finished" }
    }

    private suspend fun removeShardProgressFlag() {
        keepAliveTimerId?.let { timerId -> vertx.cancelTimer(timerId.id) }
        shardStatePersistence.flagShardNoMoreInProgress(shardId)
        logger.debug { "Remove $consumerInfo from in progress list" }
    }

    private suspend fun getQueryStartPosition(): QueryPosition {
        return when (options.shardIteratorStrategy) {
            ShardIteratorStrategy.FORCE_LATEST -> {
                logger.debug { "Force ${ShardIteratorType.LATEST.name} shard iterator on $consumerInfo" }
                QueryPosition(kinesisClient.getLatestShardIterator(options.streamName, shardId), null)
            }
            ShardIteratorStrategy.EXISTING_OR_LATEST -> {
                val existingSequenceNumber = shardStatePersistence.getConsumerShardSequenceNumber(shardId)
                if (existingSequenceNumber.isNotNull()) {
                    logger.debug { "Use existing shard sequence number: \"$existingSequenceNumber\" for $consumerInfo" }
                    QueryPosition(
                        kinesisClient.getShardIteratorBySequenceNumber(
                            options.streamName,
                            shardId,
                            existingSequenceNumber
                        ), existingSequenceNumber
                    )
                } else {
                    logger.debug { "Use ${ShardIteratorType.LATEST.name} shard iterator for $consumerInfo because no existing position found" }
                    QueryPosition(kinesisClient.getLatestShardIterator(options.streamName, shardId), null)
                }
            }
        }
    }

    private suspend fun deliver(records: List<Record>) {
        suspendCancellableCoroutine<Unit> { cont ->
            onRecords(records, Handler {
                if (it.succeeded()) {
                    cont.resume(Unit)
                } else {
                    cont.resumeWithException(it.cause())
                }
            })
        }
    }

    private fun retryFromFailedRecord(exception: Throwable) =
        if (exception is KinesisConsumerException && exception.errorHandling.isNotNull()) {
            exception.errorHandling == ErrorHandling.RETRY_FROM_FAILED_RECORD
        } else options.errorHandling == ErrorHandling.RETRY_FROM_FAILED_RECORD

    /**
     * Concrete consumer verticle implementations should / can implement final Kinesis record handling here.
     */
    protected abstract fun onRecords(records: List<Record>, handler: Handler<AsyncResult<Void>>)

    private val consumerInfo by lazy { "{ application: \"${options.applicationName}\", stream: \"${options.streamName}\", shard: \"${shardId}\", verticle: \"${this::class.java.name}\" }" }

}

data class QueryPosition(val iterator: ShardIterator, val sequenceNumber: SequenceNumber?)
