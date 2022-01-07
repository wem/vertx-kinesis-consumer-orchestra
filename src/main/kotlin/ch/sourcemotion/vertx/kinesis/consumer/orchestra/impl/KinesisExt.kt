package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.*


/**
 * @return Description of Kinesis stream for given [streamName]. This function will wait until the stream
 * has status [StreamStatus.ACTIVE] so we are based on actual state.
 */
internal suspend fun KinesisAsyncClient.streamDescriptionWhenActiveAwait(streamName: String): StreamDescription {
    var description: StreamDescription? = null
    while (description?.streamStatus() != StreamStatus.ACTIVE) {
        description = runCatching { streamDescriptionAwait(streamName) }.getOrElse {
            if (it is LimitExceededException) {
                delay(300)
                null
            } else {
                throw it
            }
        }
    }
    return description
}

internal suspend fun KinesisAsyncClient.listShardsRateLimitingAware(streamName: String): List<Shard> {
    var shards: List<Shard>? = null
    while (shards == null) {
        shards = runCatching { listShards { it.streamName(streamName) }.await().shards() }.getOrElse {
            if (it is LimitExceededException) {
                delay(100)
                null
            } else {
                throw it
            }
        }
    }
    return shards
}

private suspend fun KinesisAsyncClient.streamDescriptionAwait(streamName: String): StreamDescription = describeStream {
    it.streamName(streamName)
}.await().streamDescription()

internal suspend fun KinesisAsyncClient.getLatestShardIteratorAwait(
    streamName: String,
    shardId: ShardId
) = getShardIteratorAwait(streamName, ShardIteratorType.LATEST, shardId)

internal suspend fun KinesisAsyncClient.getShardIteratorAwait(
    streamName: String,
    shardIteratorType: ShardIteratorType,
    shardId: ShardId,
    sequenceNumber: SequenceNumber? = null
): ShardIterator {
    return getShardIterator { builder ->
        builder.streamName(streamName)
        builder.shardId(shardId.id)
        builder.shardIteratorType(shardIteratorType)
        sequenceNumber?.let { builder.startingSequenceNumber(it.number) }
    }.await().shardIterator().asShardIteratorTyped()
}

internal suspend fun KinesisAsyncClient.getShardIteratorBySequenceNumberAwait(
    streamName: String,
    shardId: ShardId,
    sequenceNumber: SequenceNumber
): ShardIterator {
    val iteratorType = if (sequenceNumber.iteratorPosition == SequenceNumberIteratorPosition.AFTER) {
        ShardIteratorType.AFTER_SEQUENCE_NUMBER
    } else {
        ShardIteratorType.AT_SEQUENCE_NUMBER
    }
    return getShardIteratorAwait(streamName, iteratorType, shardId, sequenceNumber)
}
