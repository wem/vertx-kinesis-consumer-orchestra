package ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.service.ServiceProxyManager
import com.fasterxml.jackson.annotation.JsonProperty
import io.vertx.codegen.annotations.DataObject
import io.vertx.codegen.annotations.GenIgnore
import io.vertx.codegen.annotations.ProxyGen
import io.vertx.core.Future
import io.vertx.core.json.JsonObject

@ProxyGen
interface ConsumerControlService {
    @GenIgnore
    companion object : ServiceProxyManager<ConsumerControlService>(
        "/kinesis-consumer-orchester/consumer-control/service",
        ConsumerControlService::class.java
    )

    /**
     * Stops the consumer of an explicit shard.
     */
    fun stopConsumer(shardId: ShardId): Future<Void>

    /**
     * Stops the amount of consumer for the given [consumerCount].
     *
     * @return Result of the stop command which contains the ids of the stopped shards and the number of still active consumers.
     */
    fun stopConsumers(consumerCount: Int): Future<StopConsumersCmdResult>

    /**
     * Starts consumers for given shards. The implementation has to determine the shard iterator (strategy) to use.
     *
     * @return Count of currently active consumers
     */
    fun startConsumers(shardIds: List<ShardId>): Future<Int>
}

@DataObject
data class StopConsumersCmdResult(
    @field:JsonProperty("stoppedShardIds") val stoppedShardIds: List<ShardId>,
    @field:JsonProperty("activeConsumers") val activeConsumers: Int
) {
    constructor(json: JsonObject) : this(
        json.getJsonArray("stoppedShardIds").map {
            if (it is String) {
                ShardId(it)
            } else {
                ShardId(it as JsonObject)
            }
        },
        json.getInteger("activeConsumers")
    )

    fun toJson(): JsonObject = JsonObject.mapFrom(this)
}