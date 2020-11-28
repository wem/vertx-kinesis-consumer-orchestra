package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.getChildShards
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.StreamDescription

/**
 * The signal for resharding in any case is an empty shard iterator. As the reaction of this software differs for both cases (slit, merge),
 * we need to wait until we can identify the kind of resharding.
 */
class ReshardingEventFactory(
    private val streamDescription: StreamDescription,
    private val finishedShardId: ShardId
) {

    fun createReshardingEvent(): ReshardingEvent {
        val childShards = finishedShardId.getChildShards(streamDescription.shards())
        // Resharding event with 1 child = shard merge, 2 parents = shard split
        return when (childShards.size) {
            1 -> createMergeReshardingEvent(childShards.first())
            2 -> createSplitReshardingEvent(childShards)
            else -> throw VertxKinesisConsumerOrchestraException("Unable to determine a valid count of child shards of finished shard $finishedShardId")
        }
    }

    private fun createMergeReshardingEvent(childShard: Shard) =
        MergeReshardingEvent(finishedShardId, childShard.shardIdTyped())

    private fun createSplitReshardingEvent(childShards: List<Shard>): SplitReshardingEvent =
        SplitReshardingEvent(finishedShardId, childShards.map { it.shardIdTyped() })

}
