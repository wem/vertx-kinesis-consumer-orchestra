package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIdTyped

object ShardIdGenerator {
    fun generateShardId(shardNumber: Int = 0): ShardId =
        ("shardId-" + "$shardNumber".padStart(12, '0')).asShardIdTyped()

    fun generateShardIdList(shardIdCount: Int, shardNumberOffset: Int = 0): ShardIdList =
        Array(shardIdCount) { shardNumber ->
            generateShardId(
                shardNumber + shardNumberOffset
            )
        }.toList()

    fun reshardingIdConstellation() = Triple(generateShardId(0), generateShardId(1), generateShardId(2))

}
