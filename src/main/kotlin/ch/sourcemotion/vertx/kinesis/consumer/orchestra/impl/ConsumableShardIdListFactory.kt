package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isResharded
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.parentShardIds
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import software.amazon.awssdk.services.kinesis.model.Shard


/**
 * Factory to create list of shards they should / could be consumed, according a given maximum.
 * Also the rule will be applied, that children of parents they are not finished or not available will not get consumed yet.
 */
internal object ConsumableShardIdListFactory {
    fun create(
        existingShards: ShardList,
        /**
         * Available shards are those they are not finished and not in progress.
         */
        availableShards: ShardList,
        /**
         * The list of finished shards are used to exclude the available child shards their
         * parents are not finished yet.
         */
        finishedShardIds: ShardIdList
    ): ShardIdList {
        val existingShardIdList = existingShards.map { it.shardIdTyped() }
        val availableShardIds = availableShards.map { shard -> shard.shardIdTyped() }
        val shardIdsToConsume = mutableListOf<ShardId>()
        val pendingShards =
            availableShards.toMutableList()
                .removeParentShards() // Remove parents to avoid that they appear 2 times, one time by first level reference and by child
                .removeChildrenWithUnavailableParents(existingShardIdList, finishedShardIds, availableShardIds)

        val notFinishedParentsByChildren = getNotFinishedParentsByChildren(
            pendingShards.filter { it.isResharded() },
            availableShardIds,
            finishedShardIds
        )

        notFinishedParentsByChildren.forEach { (child, parents) ->
            if (parents.isNotEmpty()) {
                shardIdsToConsume.addAll(parents)
            } else {
                shardIdsToConsume.add(child)
            }
            pendingShards.removeAll { child == it.shardIdTyped() }
            pendingShards.removeAll { parents.contains(it.shardIdTyped()) }
        }

        shardIdsToConsume.addAll(
            pendingShards.filterNot { finishedShardIds.contains(it.shardIdTyped()) }.map { it.shardIdTyped() }
        )

        return shardIdsToConsume.distinct()

    }

    private fun getNotFinishedParentsByChildren(
        mergedChildren: ShardList,
        availableShardIds: ShardIdList,
        finishedShardIds: ShardIdList
    ): Map<ShardId, ShardIdList> = mergedChildren.associate { mergeChild ->
        mergeChild.shardIdTyped() to mergeChild.parentShardIds()
            .filter { parentShardId -> availableShardIds.contains(parentShardId) }
            .filterNot { parentShardId -> finishedShardIds.contains(parentShardId) }
    }

    private fun MutableList<Shard>.removeParentShards() = apply {
        flatMap { it.parentShardIds() }.forEach { parentShardId ->
            removeIf { it.shardIdTyped() == parentShardId }
        }
    }

    private fun MutableList<Shard>.removeChildrenWithUnavailableParents(
        existingShardIds: ShardIdList,
        finishedShardIds: ShardIdList,
        availableShardIds: ShardIdList
    ) = apply {
        removeIf { childShard ->
            // We only consider existing parent shards
            val existingParentShardIds = existingShardIds.intersect(childShard.parentShardIds())
            childShard.isResharded() && existingParentShardIds
                .all { existingParentShardId -> finishedShardIds.contains(existingParentShardId) || availableShardIds.contains(existingParentShardId) }.not()
        }
    }
}
