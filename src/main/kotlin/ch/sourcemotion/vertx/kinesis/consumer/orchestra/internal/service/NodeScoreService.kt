package ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.service.ServiceProxyManager
import com.fasterxml.jackson.annotation.JsonProperty
import io.vertx.codegen.annotations.DataObject
import io.vertx.codegen.annotations.GenIgnore
import io.vertx.codegen.annotations.ProxyGen
import io.vertx.core.Future
import io.vertx.core.json.JsonObject

/**
 * Service for scoring VKCO nodes. Score means the count of consumed shards at a time.
 */
@ProxyGen
interface NodeScoreService {
    @GenIgnore
    companion object : ServiceProxyManager<NodeScoreService>(
        "/kinesis-consumer-orchester/node-state/service",
        NodeScoreService::class.java
    )

    /**
     * Set the score of this node for the given value.
     */
    fun setThisNodeScore(score: Int): Future<Void>

    /**
     * @return The scores of all currently running nodes.
     */
    fun getNodeScores(): Future<List<NodeScoreDto>>
}

@DataObject
data class NodeScoreDto(
    @field:JsonProperty("nodeId") val clusterNodeId: OrchestraClusterNodeId,
    @field:JsonProperty("score") val score: Int
) {
    constructor(json: JsonObject) : this(OrchestraClusterNodeId.of(json.getString("nodeId")), json.getInteger("score"))

    fun toJson(): JsonObject = JsonObject.mapFrom(this)
}