package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.awaitSuspending
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
}.awaitSuspending().streamDescription()

suspend fun KinesisAsyncClient.getLatestShardIterator(
    streamName: String,
    shardId: ShardId
) = getShardIterator(streamName, ShardIteratorType.LATEST, shardId)

suspend fun KinesisAsyncClient.getShardIterator(
    streamName: String,
    shardIteratorType: ShardIteratorType,
    shardId: ShardId
): String {
    return getShardIterator { builder ->
        builder.streamName(streamName)
        builder.shardId(shardId.id)
        builder.shardIteratorType(shardIteratorType)
    }.awaitSuspending().shardIterator()
}

suspend fun KinesisAsyncClient.getShardIteratorAtSequenceNumber(
    streamName: String,
    shardId: ShardId,
    sequenceNumber: String
): String {
    return getShardIterator { builder ->
        builder.streamName(streamName)
        builder.shardId(shardId.id)
        builder.shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
        builder.startingSequenceNumber(sequenceNumber)
    }.awaitSuspending().shardIterator()
}


