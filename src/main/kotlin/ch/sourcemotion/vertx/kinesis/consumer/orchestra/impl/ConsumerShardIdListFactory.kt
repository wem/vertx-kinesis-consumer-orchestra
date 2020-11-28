package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isResharded
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.parentShardIds
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped


/**
 * Factory to create list of shards they should / could be consumed, according a maximum.
 * Also the rule will be applied, that children of parents they are not finished will not get consumed yet.
 */
object ConsumerShardIdListFactory {
    fun create(
        /**
         * Avaiable shards are those they are not finished and not in progress.
         */
        availableShards: ShardList,
        /**
         * The list of finished shards are used to exclude the available child shards their
         * parents are not finished yet.
         */
        finishedShardIds: ShardIdList,
        maxShardCount: Int,
    ): ShardIdList {
        val availableShardIds = availableShards.map { shard -> shard.shardIdTyped() }
        val shardIdsToConsume = mutableListOf<ShardId>()
        val pendingShards = availableShards.toMutableList()

        val notFinishedMergeParentsByChildren = getNotFinishedParentsByChildren(
            pendingShards.filter { it.isResharded() },
            availableShardIds,
            finishedShardIds
        )

        notFinishedMergeParentsByChildren.forEach { (child, parents) ->
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

        return shardIdsToConsume.distinct().adjustList(maxShardCount)
    }

    private fun getNotFinishedParentsByChildren(
        mergedChildren: ShardList,
        availableShardIds: ShardIdList,
        finishedShardIds: ShardIdList
    ): Map<ShardId, ShardIdList> =
        mergedChildren.associate {
            it.shardIdTyped() to it.parentShardIds().filter { parentShardId ->
                availableShardIds.contains(parentShardId)
            }.filterNot { parentShardId -> finishedShardIds.contains(parentShardId) }
        }

    private fun ShardIdList.adjustList(maxShardCount: Int): ShardIdList = take(maxShardCount)
}
