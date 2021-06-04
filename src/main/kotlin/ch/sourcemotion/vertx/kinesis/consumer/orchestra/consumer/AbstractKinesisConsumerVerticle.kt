package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching.Fetcher
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching.RecordBatch
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching.RecordBatchStreamReader
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
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

    private lateinit var fetcher: Fetcher
    private lateinit var recordBatchReader: RecordBatchStreamReader
    private var inProgressJobId: Long? = null

    override suspend fun start() {
        startConsumer()
    }

    override suspend fun stop() {
        running = false
        logger.info { "Stopping Kinesis consumer verticle on $consumerInfo" }
        runCatching { fetcher.stop() }
        runCatching { inProgressJobId?.let { vertx.cancelTimer(it) } }
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
            throw VertxKinesisConsumerOrchestraException(
                "Unable to lookup start position of consumer \"$consumerInfo\"",
                it
            )
        }

        fetcher = Fetcher.of(
            vertx, options.fetcherOptions, options.clusterName, startFetchPosition,
            this, shardId, kinesisClient
        ).also {
            // We start right fetcher here, so we can handle startup issues during deployment
            it.start()
        }

        recordBatchReader = fetcher.streamReader

        runCatching { beginFetching() }
            .getOrElse {
                throw VertxKinesisConsumerOrchestraException("Unable to begin fetching on \"$consumerInfo\"", it)
            }

        logger.info { "Kinesis consumer verticle started on \"$consumerInfo\"" }
    }

    private fun startShardInProgressKeepAlive() {
        inProgressJobId = vertx.setPeriodic(options.shardProgressExpirationMillis / 3) {
            launch {
                if (running) {
                    shardStatePersistence.flagShardInProgress(options.shardId)
                }
            }
        }
    }

    private fun beginFetching() {
        running = true
        launch {
            while (running) {
                runCatching {
                    recordBatchReader.readFromStream()
                }.onSuccess { recordBatch ->
                    val records = recordBatch.records
                    tryDeliverRecords(records, recordBatch)
                }.onFailure { throwable ->
                    logger.warn(throwable) { "Failure during fetching records on $consumerInfo ... but will continue" }
                }
            }
            logger.info { "$consumerInfo is no more consuming" }
        }
    }

    private suspend fun tryDeliverRecords(records: List<Record>, recordBatch: RecordBatch) {
        val (successfullyDelivered, recordsToRetry) = deliverWithFailureHandling(records)

        if (successfullyDelivered) {
            val latestSequenceNumber = recordBatch.sequenceNumber
            val nextPosition = if (recordBatch.nextShardIterator != null) {
                FetchPosition(recordBatch.nextShardIterator, latestSequenceNumber)
            } else null // Means shard got resharded and did end

            if (latestSequenceNumber.isNotNull()) {
                saveDeliveredSequenceNbr(latestSequenceNumber)
            }

            if (nextPosition == null && recordBatchReader.isEmpty()) {
                onResharding(recordBatch.childShards)
            }
        } else if (recordsToRetry.isNotEmpty()) {
            val firstRecordToRetry = recordsToRetry.first()
            val lastSuccessful = records.toList().takeWhile { it.sequenceNumber() != firstRecordToRetry.sequenceNumber() }.lastOrNull()
            if (lastSuccessful != null) {
                saveDeliveredSequenceNbr(lastSuccessful.sequenceNumber().asSequenceNumberAfter())
            }
            tryDeliverRecords(recordsToRetry, recordBatch)
        } else {
            records.lastOrNull()?.let {
                saveDeliveredSequenceNbr(it.sequenceNumber().asSequenceNumberAfter())
            }
        }
    }

    private suspend fun deliverWithFailureHandling(
        records: List<Record>,
    ): Pair<Boolean, List<Record>> = runCatching {
        deliver(records)
        true to emptyList<Record>()
    }.getOrElse { throwable ->
        false to handleConsumerFailure(throwable, records)
    }

    private fun handleConsumerFailure(
        throwable: Throwable,
        records: List<Record>
    ): List<Record> {
        return if (shouldRetryFromFailedRecord(throwable)) {
            if (throwable is KinesisConsumerException) {
                val failedSequence = throwable.failedRecord.sequenceNumber.asSequenceNumberAt()
                logger.debug {
                    "Record processing did fail on \"$consumerInfo\". All information available to retry" +
                            "from failed record."
                }
                val failedRecordIdx = records.indexOfFirst { it.sequenceNumber() == failedSequence.number }
                return records.toList().subList(failedRecordIdx, records.size)
            } else { // Retry from last successful shard iterator
                logger.warn(throwable) {
                    "Kinesis consumer configured to retry from failed record, but no record " +
                            "information available as wrong exception type \"${throwable::class.java.name}\" was thrown. " +
                            "To retry consume, beginning from failed record you must throw an exception of type " +
                            "\"${KinesisConsumerException::class.java.name}\", so we retry from latest successful fetch position"
                }
                records
            }
        } else emptyList() // Fallback: Ignore and continue
    }

    /**
     * Called when the shard iterator of the consumed shard did return null. That state is the signal that the shard got
     * resharded and did end.
     */
    private suspend fun onResharding(childShards: List<ChildShard>) {
        // The shard consumed flag should stay true here, otherwise the shard could be consumed concurrently.
        logger.info {
            "Streaming on $consumerInfo ended because shard iterator did reach its end. Resharding did happen. " +
                    "Consumer(s) will be started recently according new shard setup."
        }
        running = false
        runCatching { fetcher.stop() }
        runCatching { ReshardingEvent.create(shardId, childShards.map { it.shardIdTyped() }) }
            .onSuccess { vertx.eventBus().send(EventBusAddr.resharding.notification, it) }
            .onFailure { logger.warn(it) { "Unable to start resharding workflow properly. Please restart this VKCO instance." } }
    }


    private suspend fun saveDeliveredSequenceNbr(sequenceNumber: SequenceNumber) =
        shardStatePersistence.saveConsumerShardSequenceNumber(shardId, sequenceNumber)


    private suspend fun deliver(records: List<Record>) {
        suspendCancellableCoroutine<Unit> { cont ->
            onRecords(records) {
                if (it.succeeded()) {
                    cont.resume(Unit)
                } else {
                    cont.resumeWithException(it.cause())
                }
            }
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
