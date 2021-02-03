package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import software.amazon.awssdk.services.kinesis.model.Shard

internal typealias ShardList = List<Shard>
internal typealias ShardIdList = List<ShardId>

internal data class OrchestraClusterName(val applicationName: String, val streamName: String) {
    override fun toString() = "$applicationName-$streamName"
}

internal data class ShardIterator(val iter: String) {
    companion object {
        fun of(iteratorValue: String?) = if (iteratorValue.isNotNullOrBlank()) {
            ShardIterator(iteratorValue)
        } else null
    }

    override fun toString() = iter
}

internal fun String.asShardIteratorTyped() = ShardIterator(this)

/**
 * [iteratorPosition] determines the position on which shard iterator should be queried from Kinesis for the [number].
 */
data class SequenceNumber(val number: String, val iteratorPosition: SequenceNumberIteratorPosition) {
    companion object {
        fun after(sequenceNumber: String?) = if (sequenceNumber.isNotNullOrBlank()) {
            SequenceNumber(sequenceNumber, SequenceNumberIteratorPosition.AFTER)
        } else null
    }
}

enum class SequenceNumberIteratorPosition { AFTER, AT }

internal fun String.asSequenceNumberAt() = SequenceNumber(this, SequenceNumberIteratorPosition.AT)
internal fun String.asSequenceNumberAfter() = SequenceNumber(this, SequenceNumberIteratorPosition.AFTER)

data class ShardId(val id: String) {
    override fun toString() = id
}

internal fun String.asShardIdTyped(): ShardId = ShardId(this)
