package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.getShardIteratorBySequenceNumberAwait
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse

class RecordFetcher(
    private val kinesisClient: KinesisAsyncClient,
    private val recordsPerPollLimit: Int,
    private val streamName: String,
    private val shardId: ShardId,
    private val scope: CoroutineScope,
    private val fetchDelayMillis: Long,
    private val consumerInfoSupplier: () -> String
) {
    private companion object : KLogging()

    private var fetchRecordsJob: Deferred<GetRecordsResponse>? = null

    /**
     * Cancel of the previous fetching job and start a new on the give position.
     */
    fun restartFetching(positionToFetch: QueryPosition, wait: Boolean = true) {
        cancelPendingFetchJob("Fetching restart")
        fetchNextRecords(positionToFetch, wait)
    }

    fun fetchNextRecords(positionToFetch: QueryPosition, wait: Boolean = true) {
        fetchRecordsJob = scope.async {
            if (wait) {
                delay(fetchDelayMillis)
            }
            // CancelFetchJobException should be catched by caller. This should never happen on deffered.await / getNextRecords()
            fetchNextRecords(positionToFetch)
        }
    }

    /**
     * Finally fetching of records. Catches the case when the shard iterator did expire. In this case, the iterator will be
     * refreshed by the [positionToFetch.sequenceNumber] and fetch will be retried afterwards.
     */
    private suspend fun fetchNextRecords(positionToFetch: QueryPosition): GetRecordsResponse {
        return runCatching {
            kinesisClient.getRecords {
                it.shardIterator("${positionToFetch.iterator}")
                it.limit(recordsPerPollLimit)
            }.await()
                .also { logger.trace { "Got get records response from Kinesis with \"${it.records().size}\" records" } }
        }.getOrElse { throwable ->
            return if (throwable is ExpiredIteratorException) {
                logger.info { "Shard iterator of \"${consumerInfoSupplier()}\" did expire, try to refresh and retry." }
                val sequenceNumber = positionToFetch.sequenceNumber
                if (sequenceNumber.isNotNull()) {
                    val freshIterator =
                        kinesisClient.getShardIteratorBySequenceNumberAwait(streamName, shardId, sequenceNumber)
                    fetchNextRecords(positionToFetch.copy(iterator = freshIterator))
                } else {
                    logger.error(throwable) { "Unable to refresh shard iterator, as no sequence number is known." }
                    throw throwable
                }
            } else {
                throw throwable
            }
        }
    }

    suspend fun close() {
        cancelPendingFetchJob("Fetcher closed")
        fetchRecordsJob?.join()
        fetchRecordsJob = null
    }

    private fun cancelPendingFetchJob(reason: String) {
        fetchRecordsJob?.runCatching {
            cancel(CancelFetchJobException(reason))
        }
    }

    suspend fun getNextRecords(): GetRecordsResponse = fetchRecordsJob?.await()
        ?: throw WrongFetchingOrderException("Please call prepareFetchRecords() before getNextRecords()")

    class CancelFetchJobException(message: String) : CancellationException(message)
    class WrongFetchingOrderException(msg: String) : Exception(msg)
}
