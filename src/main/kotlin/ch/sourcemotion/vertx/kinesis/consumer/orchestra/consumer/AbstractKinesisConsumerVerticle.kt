package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingEventFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.Job
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.time.Duration
import kotlin.LazyThreadSafetyMode.NONE
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Verticle which must be implemented to receive records fetched from Kinesis.
 *
 * Is configurable in orchestra options [ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions#consumerVerticleClass]
 */
abstract class AbstractKinesisConsumerVerticle : CoroutineVerticle() {

    companion object : KLogging() {
        internal const val CONSUMER_START_CMD_ADDR = "/kinesis-consumer-orchester/start-consumer"
    }

    private val options: KinesisConsumerVerticleOptions by lazy(NONE) {
        config.mapTo(KinesisConsumerVerticleOptions::class.java)
    }

    private val recordFetcher: RecordFetcher by lazy(NONE) {
        RecordFetcher(
            kinesisClient,
            options.recordsPerBatchLimit,
            options.streamName,
            shardId,
            this,
            options.kinesisFetchIntervalMillis
        ) { consumerInfo }
    }

    private val kinesisClient: KinesisAsyncClient by lazy(NONE) {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    private val shardStatePersistenceService: ShardStatePersistenceServiceAsync by lazy(NONE) {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
    }

    protected lateinit var shardId: ShardId

    /**
     * Running flag, if the verticle is still running. When the verticle  get stopped, this flag must be false.
     * Volatile because this flag is queried within coroutine, but set on "direct" event loop thread.
     */
    @Volatile
    private var running = false
    private var fetchJob: Job? = null

    override suspend fun start() {
        vertx.eventBus().localConsumer(CONSUMER_START_CMD_ADDR, this::onStartConsumerCmd)
        logger.debug { "Kinesis consumer verticle ready to get started" }
    }

    override suspend fun stop() {
        running = false
        logger.info { "Stopping Kinesis consumer verticle on $consumerInfo" }
        suspendCancellableCoroutine<Unit> { cont ->
            fetchJob?.invokeOnCompletion {
                if (it.isNotNull()) {
                    logger.warn(it) { "\"$consumerInfo\" stopped exceptionally" }
                } else {
                    logger.debug { "\"$consumerInfo\" stopped" }
                }
                launch {
                    recordFetcher.runCatching { close() }
                    cont.resume(Unit)
                }
            }
        }
        runCatching {
            removeShardProgressFlag()
        }.onSuccess { logger.info { "Kinesis consumer verticle stopped successfully on $consumerInfo" } }
            .onFailure { logger.warn { "Unable to remove shard progress flag on $consumerInfo" } }
        runCatching { kinesisClient.close() }.onFailure { logger.warn(it) { "Unable to close Kinesis client." } }
    }

    /**
     * Consumer function which will be called when this verticle should start fetching records from Kinesis.
     */
    private fun onStartConsumerCmd(msg: Message<String>) {
        shardId = msg.body().asShardIdTyped()

        logger.debug { "Try to start consumer on $consumerInfo" }

        launch {
            shardStatePersistenceService.startShardProgressAndKeepAlive(shardId)

            val startFetchPosition = StartFetchPositionLookup(
                vertx,
                consumerInfo,
                shardId,
                options,
                shardStatePersistenceService,
                kinesisClient
            ).getStartFetchPosition()

            running = true

            startFetching(startFetchPosition)
        }.invokeOnCompletion { throwable ->
            throwable?.let {
                logger.error(it) { "Unable to start Kinesis consumer verticle on \"$consumerInfo\"" }
                msg.fail(0, it.message)
            } ?: msg.reply(null)
        }
    }

    private suspend fun startFetching(fetchStartPosition: FetchPosition) {
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

                    val finalNextFetchPosition = deliverWithFailureHandling(records, usualNextPosition, currentFetchPosition)

                    // Shard did not end
                    if (finalNextFetchPosition.isNotNull()) {
                        saveFetchPositionIfUpdated(currentFetchPosition, finalNextFetchPosition)
                        currentFetchPosition = finalNextFetchPosition
                    } else {
                        onShardDidEnd()
                    }
                }.onFailure { throwable ->
                    logger.warn(throwable) { "Failure during fetching records on $consumerInfo ... but will continue" }
                }
            }
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
                shardStatePersistenceService.saveConsumerShardSequenceNumber(
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
        val nextFetchPositionAccordingFailure = handleConsumerFailure(throwable, usualNextPosition, currentFetchPosition)
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
                    options.streamName,
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

    private suspend fun onShardDidEnd() {
        logger.debug { "Streaming on $consumerInfo ended because shard iterator did reach its end. Start resharding process..." }
        running = false
        val streamDesc = kinesisClient.streamDescriptionWhenActiveAwait(options.streamName)
        persistShardIsFinished(streamDesc)
        removeShardProgressFlag()
        shardStatePersistenceService.deleteShardSequenceNumber(shardId)

        val reshardingEvent = ReshardingEventFactory(
            streamDesc,
            shardId
        ).createReshardingEvent()

        vertx.eventBus().send(EventBusAddr.resharding.notification, reshardingEvent)
    }

    private suspend fun persistShardIsFinished(streamDesc: StreamDescription) {
        shardStatePersistenceService.saveFinishedShard(
            shardId,
            // The expiration of the shard finished flag, will be an hour after the shard retention.
            // So it's ensured that we not lose the finished flag of this shard and avoid death data.
            Duration.ofHours(streamDesc.retentionPeriodHours().toLong() + 1).toMillis()
        )
        logger.debug { "Set $consumerInfo as finished" }
    }

    private suspend fun removeShardProgressFlag() {
        shardStatePersistenceService.flagShardNoMoreInProgress(shardId)
        logger.debug { "Remove $consumerInfo from in progress list" }
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
