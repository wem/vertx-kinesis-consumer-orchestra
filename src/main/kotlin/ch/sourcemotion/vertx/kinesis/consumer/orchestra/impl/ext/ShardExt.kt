package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIdTyped
import software.amazon.awssdk.services.kinesis.model.ChildShard
import software.amazon.awssdk.services.kinesis.model.Shard

/**
 * @return True when this shard is a child shard of exactly 2 shards.
 */
fun Shard.isMergedChild() = parentShardIds().size == 2

/**
 * @return True when this shard has exactly one parent shard.
 */
fun Shard.isSplitChild() = parentShardIds().size == 1

fun Shard.isResharded() = parentShardIds().isNotEmpty()

fun Shard.shardIdTyped() = ShardId(this.shardId())
fun ChildShard.shardIdTyped() = ShardId(this.shardId())
fun Shard.parentShardIdTyped() = this.parentShardId()?.let { ShardId(it) }
fun Shard.adjacentParentShardIdTyped() = this.adjacentParentShardId()?.let { ShardId(it) }

fun Shard.parentShardIds(): List<ShardId> = ArrayList<ShardId>().apply {
    if (parentShardId().isNotNullOrBlank()) {
        add(parentShardId().asShardIdTyped())
    }
    if (adjacentParentShardId().isNotNullOrBlank()) {
        add(adjacentParentShardId().asShardIdTyped())
    }
}

fun ShardId.getChildShards(allShards: ShardList) = allShards.filter { it.parentShardIds().contains(this) }
