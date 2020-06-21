package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId

class RedisKeyFactory(applicationName: String, streamName: String) {
    private val orchestraKeyBase = "$applicationName-$streamName"
    private val shardFinishedKeyBase = "${orchestraKeyBase}-finished-"
    private val shardProgressKeyBase = "${orchestraKeyBase}-progress-"

    fun createShardProgressFlagKey(shardId: ShardId): String = "${shardProgressKeyBase}$shardId"
    fun createShardProgressFlagKeyWildcard(): String = "${shardProgressKeyBase}*"

    fun createShardSequenceInfoKey(shardId: ShardId): String = "${orchestraKeyBase}-sequence-$shardId"

    fun createShardFinishedKey(shardId: ShardId): String = "${shardFinishedKeyBase}$shardId"
    fun createShardFinishedRedisKeyWildcard(): String = "${shardFinishedKeyBase}*"

    fun createDeploymentLockKey() = "${orchestraKeyBase}-deployment-lock"

    fun createMergeReshardingEventCountKey(childShardId: ShardId) =
        "${orchestraKeyBase}-merge-reshard-event-${childShardId.id}"
}
