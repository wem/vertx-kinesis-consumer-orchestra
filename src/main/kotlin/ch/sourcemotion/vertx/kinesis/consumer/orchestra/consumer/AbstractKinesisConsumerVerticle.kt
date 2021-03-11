package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching.DynamicRecordFetcher
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching.RecordBatchStreamReader
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.adjacentParentShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.parentShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import mu.KLogger
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record
import kotlin.LazyThreadSafetyMode.NONE
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Verticle which must be implemented to receive records fetched from Kinesis.
 *
 * Is configurable in orchestra options [ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions#consumerVerticleClass]
 */
abstract class AbstractKinesisConsumerVerticle : CoroutineVerticle() {

    private companion object : KLogging()

    private val options: KinesisConsumerVerticleOptions by lazy(NONE) {
        config.mapTo(KinesisConsumerVerticleOptions::class.java)
    }

    private val kinesisClient: KinesisAsyncClient by lazy(NONE) {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    private val shardStatePersistence: ShardStatePersistenceServiceAsync by lazy(NONE) {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
    }

    protected val shardId: ShardId by lazy(NONE) { options.shardId }

    /**
     * Running flag, if the verticle is still running. When the verticle  get stopped, this flag must be false.
     * Volatile because this flag is queried within coroutine, but set on "direct" event loop thread.
     */
    @Volatile
    private var running = false

    private lateinit var fetcher: DynamicRecordFetcher
    private lateinit var recordBatchReader: RecordBatchStreamReader

    override suspend fun start() {
        startConsumer()
    }

    override suspend fun stop() {
        running = false
        logger.info { "Stopping Kinesis consumer verticle on $consumerInfo" }
        runCatching { fetcher.stop() }
        runCatching {
            shardStatePersistence.flagShardNoMoreInProgress(shardId)
        }.onSuccess { logger.info { "Kinesis consumer verticle stopped successfully on $consumerInfo" } }
            .onFailure { logger.warn(it) { "Unable to remove shard progress flag on $consumerInfo" } }
    }

    /**
     * Consumer function which will be called when this verticle should start fetching records from Kinesis.
     */
    private suspend fun startConsumer() {
        logger.debug { "Try to start consumer on $consumerInfo" }

        shardStatePersistence.flagShardInProgress(options.shardId)
        startShardInProgressKeepAlive()

        val startFetchPosition = runCatching {
            StartFetchPositionLookup(
                vertx,
                consumerInfo,
                shardId,
                options,
                shardStatePersistence,
                kinesisClient
            ).getStartFetchPosition()
        }.getOrElse {
            throw VertxKinesisConsumerOrchestraException("Unable to lookup start position of consumer \"$consumerInfo\"", it)
        }

        fetcher = DynamicRecordFetcher(
            options.fetcherOptions,
            startFetchPosition,
            this,
            options.clusterName.streamName,
            shardId,
            kinesisClient
        )
        recordBatchReader = fetcher.streamReader

        runCatching { beginFetching(startFetchPosition) }
            .getOrElse {
                throw VertxKinesisConsumerOrchestraException("Unable to begin fetching on \"$consumerInfo\"", it)
            }

        logger.info { "Kinesis consumer verticle started on \"$consumerInfo\"" }
    }

    private fun startShardInProgressKeepAlive() {
        vertx.setPeriodic(options.shardProgressExpirationMillis / 3) {
            launch {
                if (running) {
                    shardStatePersistence.flagShardInProgress(options.shardId)
                }
            }
        }
    }

    private fun beginFetching(startPosition: FetchPosition) {
        var previousPosition: FetchPosition = startPosition
        fetcher.start()
        running = true
        launch(ConsumerLoggingContext(logger, options.shardId)) {
            while (running) {
                runCatching {
                    recordBatchReader.readFromStream()
                }.onSuccess { recordBatch ->
                    val records = recordBatch.records

                    val (successfullyDelivered, positionToContinueAfterFailure) = deliverWithFailureHandling(
                        records,
                        previousPosition
                    )

                    if (successfullyDelivered) {
                        val latestSequenceNumber = recordBatch.sequenceNumber
                        val nextPosition = if (recordBatch.nextShardIterator != null) {
                            FetchPosition(recordBatch.nextShardIterator, latestSequenceNumber)
                        } else null // Means shard got resharded and did end

                        if (latestSequenceNumber.isNotNull()) {
                            saveDeliveredSequenceNbr(latestSequenceNumber)
                        }

                        if (nextPosition != null) {
                            previousPosition = nextPosition
                        } else {
                            onResharding()
                        }
                    } else if (positionToContinueAfterFailure != null) {
                        val sequenceNumberToContinueFrom = positionToContinueAfterFailure.sequenceNumber
                        if (sequenceNumberToContinueFrom != null) {
                            saveDeliveredSequenceNbr(sequenceNumberToContinueFrom)
                        }
                        fetcher.resetTo(positionToContinueAfterFailure)
                        previousPosition = positionToContinueAfterFailure
                    } // In the case of a failed delivery and the position to continue after failure, we simple continue.
                }.onFailure { throwable ->
                    logger.warn(throwable) { "Failure during fetching records on $consumerInfo ... but will continue" }
                }
            }
            logger.info { "$consumerInfo is no more consuming" }
        }
    }

    private suspend fun deliverWithFailureHandling(
        records: List<Record>,
        previousPosition: FetchPosition
    ): Pair<Boolean, FetchPosition?> = runCatching {
        deliver(records)
        true to null
    }.getOrElse { throwable ->
        false to handleConsumerFailure(throwable, previousPosition)
    }

    private suspend fun handleConsumerFailure(
        throwable: Throwable,
        previousPosition: FetchPosition
    ): FetchPosition? {
        return if (shouldRetryFromFailedRecord(throwable)) {
            if (throwable is KinesisConsumerException) {
                val failedSequence = throwable.failedRecord.sequenceNumber.asSequenceNumberAt()
                val iteratorAtFailedSequence = kinesisClient.getShardIteratorBySequenceNumberAwait(
                    options.clusterName.streamName,
                    shardId,
                    failedSequence
                )
                logger.debug {
                    "Record processing did fail on \"$consumerInfo\". All information available to retry" +
                            "from failed record."
                }
                FetchPosition(iteratorAtFailedSequence, failedSequence)
            } else { // Retry from last successful shard iterator
                logger.warn(throwable) {
                    "Kinesis consumer configured to retry from failed record, but no record " +
                            "information available as wrong exception type \"${throwable::class.java.name}\" was thrown. " +
                            "To retry consume, beginning from failed record you must throw an exception of type " +
                            "\"${KinesisConsumerException::class.java.name}\", so we retry from latest successful fetch position"
                }
                previousPosition
            }
        } else null // Fallback: Ignore and continue
    }

    /**
     * Called when the shard iterator of the consumed shard did return null. That state is the signal that the shard got
     * resharded and did end.
     */
    private suspend fun onResharding() {
        logger.debug {
            "Streaming on $consumerInfo ended because shard iterator did reach its end. Resharding did happen. " +
                    "Consumer(s) will be started recently according new shard setup."
        }
        // The shared consumed flag should stay true here, otherwise the shard could be consumed concurrently.
        running = false
        val streamDescription = kinesisClient.streamDescriptionWhenActiveAwait(options.clusterName.streamName)
        val shards = streamDescription.shards()
        val childShardIds =
            shards.filter { it.adjacentParentShardIdTyped() == options.shardId || it.parentShardIdTyped() == options.shardId }
                .map { it.shardIdTyped() }
        val reshardingEvent = ReshardingEvent.create(shardId, childShardIds)
        vertx.eventBus().send(EventBusAddr.resharding.notification, reshardingEvent)
    }


    private suspend fun saveDeliveredSequenceNbr(sequenceNumber: SequenceNumber) =
        shardStatePersistence.saveConsumerShardSequenceNumber(
            shardId,
            sequenceNumber
        )


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

    private fun shouldRetryFromFailedRecord(exception: Throwable) =
        if (exception is KinesisConsumerException && exception.errorHandling.isNotNull()) {
            exception.errorHandling == ErrorHandling.RETRY_FROM_FAILED_RECORD
        } else options.errorHandling == ErrorHandling.RETRY_FROM_FAILED_RECORD

    /**
     * Concrete consumer verticle implementations should / can implement final Kinesis record handling here.
     */
    protected abstract fun onRecords(records: List<Record>, handler: Handler<AsyncResult<Void>>)

    private val consumerInfo: String
        get() = "{ cluster: \"${options.clusterName}\", shard: \"${shardId}\", verticle: \"${this::class.java.name}\" }"
}

private class ConsumerLoggingContext(
    private val logger: KLogger,
    private val shardId: ShardId
) : ThreadContextElement<Unit>, AbstractCoroutineContextElement(Key) {
    private companion object Key : CoroutineContext.Key<ConsumerLoggingContext>
    override fun updateThreadContext(context: CoroutineContext) {
        logger.info("Kinesis consumer on shard \"$shardId\" resumed / started")
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
        logger.info("Kinesis consumer on shard \"$shardId\" suspend")
    }
}
