package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId

internal class RedisKeyFactory(private val clusterName: OrchestraClusterName) {
    private val shardFinishedKeyBase = "$clusterName-finished-"
    private val shardProgressKeyBase = "$clusterName-progress-"

    constructor(applicationName: String, streamName: String): this(OrchestraClusterName(applicationName, streamName))

    fun createShardProgressFlagKey(shardId: ShardId): String = "${shardProgressKeyBase}$shardId"

    fun createShardSequenceNumberKey(shardId: ShardId): String = "$clusterName-sequence-$shardId"

    fun createShardFinishedKey(shardId: ShardId): String = "$shardFinishedKeyBase$shardId"

    fun createDeploymentLockKey() = "$clusterName-deployment-lock"
}
