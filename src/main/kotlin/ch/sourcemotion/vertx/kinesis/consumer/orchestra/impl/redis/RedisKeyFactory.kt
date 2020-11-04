package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId

class RedisKeyFactory(applicationName: String, streamName: String) {
    private val orchestraRedisKeyBase = "$applicationName-$streamName"
    private val shardFinishedKeyBase = "$orchestraRedisKeyBase-finished-"
    private val shardProgressKeyBase = "$orchestraRedisKeyBase-progress-"

    fun createShardProgressFlagKey(shardId: ShardId): String = "${shardProgressKeyBase}$shardId"
    fun createShardProgressFlagKeyWildcard(): String = "${shardProgressKeyBase}*"

    fun createShardSequenceNumberKey(shardId: ShardId): String = "$orchestraRedisKeyBase-sequence-$shardId"

    fun createShardFinishedKey(shardId: ShardId): String = "$shardFinishedKeyBase$shardId"
    fun createShardFinishedRedisKeyWildcard(): String = "$shardFinishedKeyBase*"

    fun createDeploymentLockKey() = "$orchestraRedisKeyBase-deployment-lock"

    fun createMergeParentReadyToReshardKey(parentShardId: ShardId, childShardId: ShardId) =
        "$orchestraRedisKeyBase-ready-for-merge-$childShardId-$parentShardId"

    fun createMergeParentReadyToReshardKeyWildcard(childShardId: ShardId) =
        "$orchestraRedisKeyBase-ready-for-merge-$childShardId-*"
}
