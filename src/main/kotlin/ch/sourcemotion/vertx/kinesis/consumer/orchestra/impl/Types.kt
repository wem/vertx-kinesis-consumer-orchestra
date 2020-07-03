package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import software.amazon.awssdk.services.kinesis.model.Shard

typealias ShardList = List<Shard>
typealias ShardIdList = List<ShardId>

data class ShardIterator(val iter: String) {
    override fun toString() = iter
}

fun String.asShardIteratorTyped() = ShardIterator(this)

/**
 * [iteratorPosition] determines the position on which shard iterator should be queried from Kinesis for the [number].
 */
data class SequenceNumber(val number: String, val iteratorPosition: SequenceNumberIteratorPosition)

enum class SequenceNumberIteratorPosition { AFTER, AT }

fun String.asSequenceNumberAt() = SequenceNumber(this, SequenceNumberIteratorPosition.AT)
fun String.asSequenceNumberAfter() = SequenceNumber(this, SequenceNumberIteratorPosition.AFTER)

data class ShardId(val id: String) {
    override fun toString() = id
}

fun String.asShardIdTyped(): ShardId = ShardId(this)
