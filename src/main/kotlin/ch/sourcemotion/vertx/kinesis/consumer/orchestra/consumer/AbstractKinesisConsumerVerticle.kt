package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
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
import kotlinx.coroutines.*
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.ChildShard
import software.amazon.awssdk.services.kinesis.model.Record
import kotlin.LazyThreadSafetyMode.NONE
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

    private val recordFetcher: RecordFetcher by lazy(NONE) {
        RecordFetcher(
            kinesisClient,
            options.recordsPerBatchLimit,
            options.clusterName.streamName,
            shardId,
            this,
            options.kinesisFetchIntervalMillis
        ) { consumerInfo }
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
    private var fetchJob: Job? = null

    override suspend fun start() {
        startConsumer()
    }

    override suspend fun stop() {
        running = false
        logger.info { "Stopping Kinesis consumer verticle on $consumerInfo" }
        recordFetcher.runCatching { close() }
        suspendCancellableCoroutine<Unit> { cont ->
            fetchJob?.invokeOnCompletion {
                if (it.isNotNull()) {
                    logger.warn(it) { "\"$consumerInfo\" stopped exceptionally" }
                } else {
                    logger.debug { "\"$consumerInfo\" stopped" }
                }
                cont.resume(Unit)
            }
        }
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

        startShardInProgressKeepAlive()

        suspendCancellableCoroutine<Unit> { cont ->
            launch {
                val startFetchPosition = StartFetchPositionLookup(
                    vertx,
                    consumerInfo,
                    shardId,
                    options,
                    shardStatePersistence,
                    kinesisClient
                ).getStartFetchPosition()

                running = true

                beginFetching(startFetchPosition)
            }.invokeOnCompletion { throwable ->
                if (throwable.isNotNull()) {
                    running = false
                    logger.error(throwable) { "Unable to start Kinesis consumer verticle on \"$consumerInfo\"" }
                    cont.resumeWithException(throwable)
                } else {
                    logger.info { "Kinesis consumer verticle started on \"$consumerInfo\"" }
                    cont.resume(Unit)
                }
            }
        }
    }

    private suspend fun startShardInProgressKeepAlive() {
        shardStatePersistence.flagShardInProgress(options.shardId)
        launch {
            while (isActive && running) {
                delay(options.shardProgressExpirationMillis / 2)
                shardStatePersistence.flagShardInProgress(options.shardId)
            }
        }
    }

    private fun beginFetching(fetchStartPosition: FetchPosition) {
        logger.debug { "Start fetching for $consumerInfo" }
        var currentFetchPosition = fetchStartPosition

        // We not wait on first fetch, as the last fetch operation was a longer time ago.
        recordFetcher.fetchNextRecords(currentFetchPosition, false)

        fetchJob = launch {
            while (isActive && running) {
                runCatching {
                    recordFetcher.getNextRecords()
                }.onSuccess { recordsResponse ->
                    val records = recordsResponse.records()
                    // Can be null if the shard did end. This is the next shard iterator in a usual (not failure) case
                    val usualNextIterator = recordsResponse.nextShardIterator()?.asShardIteratorTyped()

                    // If the list of queried records was empty, we still use the previous sequence number, as it didn't change
                    val usualNextSequenceNumber = records.lastOrNull()?.sequenceNumber()?.asSequenceNumberAfter()
                        ?: currentFetchPosition.sequenceNumber

                    val usualNextPosition = usualNextIterator?.let { FetchPosition(it, usualNextSequenceNumber) }

                    // We are optimistic and prefetch
                    if (usualNextPosition.isNotNull()) {
                        recordFetcher.fetchNextRecords(usualNextPosition)
                    }

                    val finalNextFetchPosition =
                        deliverWithFailureHandling(records, usualNextPosition, currentFetchPosition)

                    // Shard did not end
                    if (finalNextFetchPosition.isNotNull()) {
                        saveFetchPositionIfUpdated(currentFetchPosition, finalNextFetchPosition)
                        currentFetchPosition = finalNextFetchPosition
                    } else {
                        onResharding(recordsResponse.childShards())
                    }
                }.onFailure { throwable ->
                    logger.warn(throwable) { "Failure during fetching records on $consumerInfo ... but will continue" }
                }
            }
            logger.info { "$consumerInfo is no more consuming" }
        }
    }

    private suspend fun saveFetchPositionIfUpdated(
        currentFetchPosition: FetchPosition,
        nextFetchPosition: FetchPosition
    ) {
        if (nextFetchPosition != currentFetchPosition) {
            // Sequence number could be null here if the initial delivery of records did fail and
            // the iterator was based just on latest, there is no sequence number available
            if (nextFetchPosition.sequenceNumber.isNotNull()) {
                shardStatePersistence.saveConsumerShardSequenceNumber(
                    shardId,
                    nextFetchPosition.sequenceNumber
                )
            }
        }
    }

    private suspend fun deliverWithFailureHandling(
        records: MutableList<Record>,
        usualNextPosition: FetchPosition?,
        currentFetchPosition: FetchPosition
    ) = runCatching {
        deliver(records)
        usualNextPosition
    }.getOrElse { throwable ->
        val nextFetchPositionAccordingFailure =
            handleConsumerFailure(throwable, usualNextPosition, currentFetchPosition)
        // If a failure did happen and the
        if (nextFetchPositionAccordingFailure.isNotNull() && nextFetchPositionAccordingFailure != usualNextPosition) {
            recordFetcher.restartFetching(nextFetchPositionAccordingFailure)
        }
        nextFetchPositionAccordingFailure
    }

    private suspend fun handleConsumerFailure(
        throwable: Throwable,
        nextPosition: FetchPosition?,
        previousPosition: FetchPosition
    ): FetchPosition? {
        return if (retryFromFailedRecord(throwable)) {
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

    /**
     * Called when the shard iterator of the consumed shard return null. That state is the signal that the shard got
     * resharded and did end.
     */
    private suspend fun onResharding(childShards: List<ChildShard>) {
        logger.debug {
            "Streaming on $consumerInfo ended because shard iterator did reach its end. Resharding did happen. " +
                    "Consumer(s) will be started recently according new shard setup."
        }
        // The shared running flag should stay here, otherwise the shard could be consumed concurrently
        running = false
        val childShardIds = if (childShards.isNotEmpty()) {
            childShards.map { it.shardIdTyped() }
        } else {
            // Fallback, if get record response did not respond child shards
            val streamDescription = kinesisClient.streamDescriptionWhenActiveAwait(options.clusterName.streamName)
            val shards = streamDescription.shards()
            shards.filter { it.adjacentParentShardIdTyped() == options.shardId || it.parentShardIdTyped() == options.shardId }
                .map { it.shardIdTyped() }
        }
        val reshardingEvent = ReshardingEvent.create(shardId, childShardIds)
        vertx.eventBus().send(EventBusAddr.resharding.notification, reshardingEvent)
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

    private val consumerInfo by lazy { "{ cluster: \"${options.clusterName}\", shard: \"${shardId}\", verticle: \"${this::class.java.name}\" }" }
}
