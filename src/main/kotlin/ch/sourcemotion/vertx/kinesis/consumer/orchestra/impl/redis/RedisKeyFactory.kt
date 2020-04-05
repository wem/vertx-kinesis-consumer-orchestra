package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId

class RedisKeyFactory(applicationName: String, streamName: String) {
    private val orchestraKeyBase = "$applicationName-$streamName"

    private fun createShardProgressKeyBase(): String = "${orchestraKeyBase}-progress_"
    fun createShardProgressFlagKey(shardId: ShardId): String = "${createShardProgressKeyBase()}$shardId"
    fun createShardProgressFlagKeyWildcard(): String = "${createShardProgressKeyBase()}*"

    fun createShardIteratorKey(shardId: ShardId): String = "${orchestraKeyBase}-iterator-$shardId"

    private fun createShardFinishedKeyBase() = "${orchestraKeyBase}-finished-"
    fun createShardFinishedKey(shardId: ShardId): String = "${createShardFinishedKeyBase()}$shardId"
    fun createShardFinishedRedisKeyWildcard(): String = "${createShardFinishedKeyBase()}*"

    fun createDeploymentLockKey() = "${orchestraKeyBase}-deployment-lock"
}
