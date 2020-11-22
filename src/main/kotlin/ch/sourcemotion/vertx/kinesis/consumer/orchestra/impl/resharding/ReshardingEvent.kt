package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList

enum class ReshardingType {
    SPLIT,
    MERGE
}

abstract class ReshardingEvent(val reshardingType: ReshardingType)

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
