package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.*


/**
 * Factory to create list of shards they should / could be consumed, according a maximum.
 * Also the rule will be applied, that children of parents they are not finished yet will not get consumed.
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

        // Add not finished merge parents for processing
        shardIdsToConsume.addAll(
            getNotFinishedMergeParents(
                pendingShards.filter { it.isMergedChild() },
                availableShardIds,
                finishedShardIds
            )
        )
        pendingShards.removeAll { shardIdsToConsume.contains(it.shardIdTyped()) }

        // Add not finished split parents for processing
        shardIdsToConsume.addAll(
            getNotFinishedSplitParents(
                pendingShards.filter { it.isSplitChild() },
                availableShardIds,
                finishedShardIds
            )
        )
        pendingShards.removeAll { shardIdsToConsume.contains(it.shardIdTyped()) }

        shardIdsToConsume.addAll(
            pendingShards.asSequence().filterNot {
                // prevent split children from processing before their parents are done
                it.isSplitChild() && shardIdsToConsume.contains(it.parentShardIdTyped())
            }.filterNot {
                it.isMergedChild() &&
                        // prevent merge children from processing before their both parents are done
                        (shardIdsToConsume.contains(it.parentShardIdTyped()) ||
                                shardIdsToConsume.contains(it.adjacentParentShardIdTyped()))
            }.map { it.shardIdTyped() }.filterNot { shardIdsToConsume.contains(it) }.toList()
        )

        return shardIdsToConsume.adjustList(maxShardCount)
    }

    /**
     * @return Shard id list of merge resharding parent shards, they are not flagged as finished and are available.
     * The available check is necessary, because when the shards are not recently created and there is no orchestra
     * persistence state present.
     */
    private fun getNotFinishedMergeParents(
        mergedChildren: ShardList,
        availableShardIds: ShardIdList,
        finishedShardIds: ShardIdList
    ): ShardIdList =
        mergedChildren.flatMap { listOf(it.parentShardIdTyped(), it.adjacentParentShardIdTyped()) }
            .filterNotNull()
            .filter { availableShardIds.contains(it) }
            .filterNot { finishedShardIds.contains(it) }

    /**
     * @return Shard id list of split resharding parent shards, they are not flagged as finished.
     * The available check is necessary, because when the shards are not recently created and there is no orchestra
     * persistence state present.
     */
    private fun getNotFinishedSplitParents(
        splitChildren: ShardList,
        availableShardIds: ShardIdList,
        finishedShardIds: ShardIdList
    ): ShardIdList =
        splitChildren.mapNotNull { it.parentShardIdTyped() }
            .filter { availableShardIds.contains(it) }
            .filterNot { finishedShardIds.contains(it) }
            .distinct() // Distinct because split parent shard is parent of two children

    private fun ShardIdList.adjustList(maxShardCount: Int): ShardIdList = take(maxShardCount)
}
