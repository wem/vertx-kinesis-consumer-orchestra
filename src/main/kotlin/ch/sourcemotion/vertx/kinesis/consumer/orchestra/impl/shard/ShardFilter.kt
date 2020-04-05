package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.adjacentParentShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.parentShardIdTyped
import software.amazon.awssdk.services.kinesis.model.Shard

object ShardFilter {

    /**
     * @return Shard where the parent AND adjacent parent is set AND the given [mergedShardId] is one of them.
     */
    fun getMergeChildShard(allShards: ShardList, mergedShardId: ShardId): Shard? = allShards
        .filter { shard -> shard.parentShardIdTyped().isNotNull() && shard.adjacentParentShardIdTyped().isNotNull() }
        .firstOrNull { shard -> shard.parentShardIdTyped() == mergedShardId || shard.adjacentParentShardIdTyped() == mergedShardId }

    /**
     * @return Shards where [splitShardId] is the ONLY ONE parent.
     */
    fun getSplitChildrenShards(allShards: ShardList, splitShardId: ShardId): ShardList = allShards
        .filter { shard -> shard.adjacentParentShardIdTyped().isNull() }
        .filter { shard -> shard.parentShardIdTyped() == splitShardId }
}
