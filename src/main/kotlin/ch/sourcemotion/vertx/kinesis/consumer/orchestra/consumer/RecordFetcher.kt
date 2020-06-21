package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.getShardIteratorBySequenceNumber
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
    private val consumerInfoSupplier: () -> String
) {
    private companion object : KLogging()

    /**
     * Fetching of records. Catches the case when the shard iterator did expire. In this case, the iterator will be
     * refreshed by the [positionToFetch.sequenceNumber].
     */
    suspend fun fetchNextRecords(positionToFetch: QueryPosition): GetRecordsResponse {
        return runCatching {
            kinesisClient.getRecords {
                it.shardIterator("${positionToFetch.iterator}")
                it.limit(recordsPerPollLimit)
            }.await()
        }.getOrElse { throwable ->
            return if (throwable is ExpiredIteratorException) {
                AbstractKinesisConsumerVerticle.logger.info { "Shard iterator of \"${consumerInfoSupplier()}\" did expire, try to refresh and retry." }
                val sequenceNumber = positionToFetch.sequenceNumber
                if (sequenceNumber.isNotNull()) {
                    val freshIterator =
                        kinesisClient.getShardIteratorBySequenceNumber(streamName, shardId, sequenceNumber)
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
}
