package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.*


/**
 * Processing bundle. A bundle should be processed at once by an orchestra instance. Either a whole bundle can get processed, or it will skipped.
 */
data class ShardProcessingBundle(val shardIds: ShardIdList) {
    companion object {
        fun createShardProcessingBundle(
            shards: ShardList,
            finishedShardIds: ShardIdList
        ): ShardProcessingBundle {
            val notFinishedShardIds = mutableListOf<ShardId>()
            val pendingShard = shards.toMutableList()

            notFinishedShardIds.addAll(
                getNotFinishedMergeParents(
                    pendingShard.filter { it.isMergedChild() },
                    finishedShardIds
                )
            )

            pendingShard.removeAll { notFinishedShardIds.contains(it.shardIdTyped()) }

            notFinishedShardIds.addAll(
                getNotFinishedSplitParents(
                    pendingShard.filter { it.isSplitChild() },
                    finishedShardIds
                )
            )

            pendingShard.removeAll { notFinishedShardIds.contains(it.shardIdTyped()) }

            notFinishedShardIds.addAll(
                pendingShard.asSequence().filterNot {
                    // prevent split parents as proceed before
                    it.isSplitChild() && notFinishedShardIds.contains(it.parentShardIdTyped())
                }.filterNot {
                    it.isMergedChild() &&
                            // prevent merge parents as proceed before
                            (notFinishedShardIds.contains(it.parentShardIdTyped()) ||
                                    notFinishedShardIds.contains(it.adjacentParentShardIdTyped()))
                }
                    .map { it.shardIdTyped() }
                    .filterNot { finishedShardIds.contains(it) }.filterNot { notFinishedShardIds.contains(it) }.toList()
            )

            return ShardProcessingBundle(notFinishedShardIds)
        }

        /**
         * @return Shard id list of merge resharding parent shards, they are not flagged as finished.
         */
        private fun getNotFinishedMergeParents(
            mergedChildren: ShardList,
            finishedShardIds: ShardIdList
        ): ShardIdList =
            mergedChildren.flatMap { listOf(it.parentShardIdTyped(), it.adjacentParentShardIdTyped()) }.filterNotNull()
                .filterNot { finishedShardIds.contains(it) }

        /**
         * @return Shard id list of split resharding parent shards, they are not flagged as finished.
         */
        private fun getNotFinishedSplitParents(
            splitChildren: ShardList,
            finishedShardIds: ShardIdList
        ): ShardIdList =
            splitChildren.mapNotNull { it.parentShardIdTyped() }.filterNot { finishedShardIds.contains(it) }
                .distinctBy { it } // Distinct because one shard could be parent of two children
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
