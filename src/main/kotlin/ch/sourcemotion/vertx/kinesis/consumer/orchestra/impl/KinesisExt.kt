package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import kotlinx.coroutines.future.await
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import software.amazon.awssdk.services.kinesis.model.StreamStatus


/**
 * @return Description of Kinesis stream for given [streamName]. This function will wait until the stream
 * has status [StreamStatus.ACTIVE] so we are based on actual state.
 */
suspend fun KinesisAsyncClient.streamDescriptionWhenActiveAwait(streamName: String): StreamDescription {
    var description = streamDescription(streamName)
    while (description.streamStatus() != StreamStatus.ACTIVE) {
        description = streamDescription(streamName)
    }
    return description
}

private suspend fun KinesisAsyncClient.streamDescription(streamName: String): StreamDescription = describeStream {
    it.streamName(streamName)
}.await().streamDescription()

suspend fun KinesisAsyncClient.getLatestShardIterator(
    streamName: String,
    shardId: ShardId
) = getShardIterator(streamName, ShardIteratorType.LATEST, shardId)

suspend fun KinesisAsyncClient.getShardIterator(
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

suspend fun KinesisAsyncClient.getShardIteratorBySequenceNumber(
    streamName: String,
    shardId: ShardId,
    sequenceNumber: SequenceNumber
): ShardIterator {
    val iteratorType = if (sequenceNumber.iteratorPosition == SequenceNumberIteratorPosition.AFTER) {
        ShardIteratorType.AFTER_SEQUENCE_NUMBER
    } else {
        ShardIteratorType.AT_SEQUENCE_NUMBER
    }
    return getShardIterator(streamName, iteratorType, shardId, sequenceNumber)
}
