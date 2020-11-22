package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.*


/**
 * Processing bundle which contains / defines the shards an orchestra instance will process / consume.
 */
@Deprecated("Will be replaced by ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ConsumerShardIdListFactory")
data class ShardProcessingBundle(val shardIds: ShardIdList) {
    companion object {
        fun create(
            availableShards: ShardList,
            finishedShardIds: ShardIdList,
            loadConfiguration: LoadConfiguration
        ): ShardProcessingBundle {
            val availableShardIds = availableShards.map { shard -> shard.shardIdTyped() }
            val processingShardIds = mutableListOf<ShardId>()
            val pendingShards = availableShards.toMutableList()

            // Add not finished merge parents for processing
            processingShardIds.addAll(
                getNotFinishedMergeParents(
                    pendingShards.filter { it.isMergedChild() },
                    finishedShardIds,
                    availableShardIds
                )
            )
            pendingShards.removeAll { processingShardIds.contains(it.shardIdTyped()) }

            // Add not finished split parents for processing
            processingShardIds.addAll(
                getNotFinishedSplitParents(
                    pendingShards.filter { it.isSplitChild() },
                    finishedShardIds,
                    availableShardIds
                )
            )
            pendingShards.removeAll { processingShardIds.contains(it.shardIdTyped()) }

            processingShardIds.addAll(
                pendingShards.asSequence().filterNot {
                    // prevent split children from processing before their parents are done
                    it.isSplitChild() && processingShardIds.contains(it.parentShardIdTyped())
                }.filterNot {
                    it.isMergedChild() &&
                            // prevent merge children from processing before their both parents are done
                            (processingShardIds.contains(it.parentShardIdTyped()) ||
                                    processingShardIds.contains(it.adjacentParentShardIdTyped()))
                }.map { it.shardIdTyped() }
                    .filterNot { finishedShardIds.contains(it) }.filterNot { processingShardIds.contains(it) }.toList()
            )

            return ShardProcessingBundle(processingShardIds).adjustBundle(loadConfiguration)
        }

        /**
         * @return Shard id list of merge resharding parent shards, they are not flagged as finished and are available.
         * The available check is necessary, because when the shards are not recently created and there is no orchestra
         * persistence state present.
         */
        private fun getNotFinishedMergeParents(
            mergedChildren: ShardList,
            finishedShardIds: ShardIdList,
            availableShardIds: ShardIdList
        ): ShardIdList =
            mergedChildren.flatMap { listOf(it.parentShardIdTyped(), it.adjacentParentShardIdTyped()) }.filterNotNull()
                .filter { availableShardIds.contains(it) }.filterNot { finishedShardIds.contains(it) }

        /**
         * @return Shard id list of split resharding parent shards, they are not flagged as finished.
         * The available check is necessary, because when the shards are not recently created and there is no orchestra
         * persistence state present.
         */
        private fun getNotFinishedSplitParents(
            splitChildren: ShardList,
            finishedShardIds: ShardIdList,
            availableShardIds: ShardIdList
        ): ShardIdList =
            splitChildren.mapNotNull { it.parentShardIdTyped() }.filterNot { finishedShardIds.contains(it) }
                .filter { availableShardIds.contains(it) }
                .distinct() // Distinct because split parent shard is parent of two children
    }

    fun adjustBundle(loadConfiguration: LoadConfiguration): ShardProcessingBundle {
        return when (loadConfiguration.strategy) {
            LoadStrategy.DO_ALL_SHARDS -> this
            LoadStrategy.EXACT -> ShardProcessingBundle(
                this.shardIds.take(
                    loadConfiguration.exactCount
                        ?: throw VertxKinesisConsumerOrchestraException("Exact load configuration needs configured count")
                )
            )
        }
    }
}
