package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.adjacentParentShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.parentShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardFilter
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.StreamDescription

/**
 * The signal for resharding in any case is an empty shard iterator. As the reaction of this software differs for both cases (slit, merge),
 * we need to wait until we can identify the kind of resharding.
 */
class ReshardingEventFactory(
    private val streamDescription: StreamDescription,
    private val streamName: String,
    private val endedShardId: ShardId
) {

    fun createReshardingEvent(): ReshardingEvent {
        val splitResultShards = ShardFilter.getSplitChildrenShards(streamDescription.shards(), endedShardId)
        // On a resharding, if there are 2 shards they have the ended shard as parent signals that it's a split resharding
        return if (splitResultShards.size == 2) {
            createSplitReshardingEvent(splitResultShards)
        } else {
            createMergeReshardingEvent(streamDescription)
        }
    }

    private fun createMergeReshardingEvent(streamDescription: StreamDescription): MergeReshardingEvent {
        val mergeChildShard = ShardFilter.getMergeChildShard(streamDescription.shards(), endedShardId)
        return mergeChildShard?.let {
            MergeReshardingEvent(
                mergeChildShard.parentShardIdTyped()!!,
                mergeChildShard.adjacentParentShardIdTyped()!!,
                mergeChildShard.shardIdTyped(),
                endedShardId
            )
        } ?: throw VertxKinesisConsumerOrchestraException(
            "Resharding did happen on stream: \"$streamName\", but it's not possible to determine if it was a split or merge"
        )
    }

    private fun createSplitReshardingEvent(splitResultShards: List<Shard>): SplitReshardingEvent =
        SplitReshardingEvent(endedShardId, splitResultShards.map { it.shardIdTyped() })

}
