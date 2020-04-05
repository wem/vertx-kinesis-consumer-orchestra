package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isFalse
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardProcessingBundle
import mu.KLogging

internal class OrchestraShardComposition(
    private val availableShards: ShardList,
    private val shardIdsInProgress: ShardIdList,
    private val finishedShardIds: ShardIdList,
    private val loadConfiguration: LoadConfiguration
) {

    private companion object : KLogging()

    fun createShardProcessingBundle(): ShardProcessingBundle {
        val shardsNotInProgress = availableShards.sortedBy { it.shardId() }
            .filter { shard -> shardIdsInProgress.contains(shard.shardIdTyped()).isFalse() }

        val processingBundle =
            ShardProcessingBundle.createShardProcessingBundle(
                shardsNotInProgress,
                finishedShardIds
            )

        return processingBundle.adjustBundle(loadConfiguration)
    }

}
