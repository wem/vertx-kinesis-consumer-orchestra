package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import software.amazon.awssdk.services.kinesis.model.Shard

/**
 * @return True when this shard is a child shard of 2 merged shards.
 */
fun Shard.isMergedChild() = parentShardId().isNotNullOrBlank() && adjacentParentShardId().isNotNullOrBlank()

/**
 * @return True when this shard is one of the children of a splitted shard.
 */
fun Shard.isSplitChild() = parentShardId().isNotNullOrBlank() && adjacentParentShardId().isNullOrBlank()

/**
 * Provides type safe shard id getter to AWs shard class instances.
 */
fun Shard.shardIdTyped() = ShardId(this.shardId())
fun Shard.parentShardIdTyped() = this.parentShardId()?.let { ShardId(it) }
fun Shard.adjacentParentShardIdTyped() = this.adjacentParentShardId()?.let { ShardId(it) }
