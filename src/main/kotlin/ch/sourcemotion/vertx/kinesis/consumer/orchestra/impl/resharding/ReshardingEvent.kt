package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList

enum class ReshardingType {
    SPLIT,
    MERGE
}

abstract class ReshardingEvent(val reshardingType: ReshardingType) {
    companion object {
        fun create(parentShardId: ShardId, childShardIds: List<ShardId>) = when (childShardIds.size) {
            1 -> MergeReshardingEvent(parentShardId, childShardIds.first())
            2 -> SplitReshardingEvent(parentShardId, childShardIds)
            else -> throw IllegalStateException("Unable to create resharding event from a parent with an unexpected count of child shards ${childShardIds.size}")
        }
    }
}

/**
 * Event fired by consumer in the case if the consumed shard got merged with another.
 */
data class MergeReshardingEvent(
    val finishedParentShardId: ShardId,
    val childShardId: ShardId
) : ReshardingEvent(ReshardingType.MERGE)

/**
 * Event fired by consumer in the case if the consumed shard got split into two shards.
 */
data class SplitReshardingEvent(
    val finishedParentShardId: ShardId,
    val childShardIds: ShardIdList
) : ReshardingEvent(ReshardingType.SPLIT)
