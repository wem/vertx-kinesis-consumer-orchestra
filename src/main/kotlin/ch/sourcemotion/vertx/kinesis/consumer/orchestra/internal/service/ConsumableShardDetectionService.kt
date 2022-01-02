package ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.service.ServiceProxyManager
import io.vertx.codegen.annotations.GenIgnore
import io.vertx.codegen.annotations.ProxyGen
import io.vertx.core.Future

/**
 * Detection of consumable shards. Consumable shards are those, they are currently not consumed by a VKCO instance / node.
 */
@ProxyGen
interface ConsumableShardDetectionService {
    @GenIgnore
    companion object : ServiceProxyManager<ConsumableShardDetectionService>(
        "/kinesis-consumer-orchester/consumable-shard-detection/service",
        ConsumableShardDetectionService::class.java
    )

    /**
     * @return Consumable shards.
     */
    fun getConsumableShards(): Future<List<ShardId>>
}