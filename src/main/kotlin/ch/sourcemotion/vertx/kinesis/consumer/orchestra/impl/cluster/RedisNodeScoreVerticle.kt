package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cluster

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.NodeScoreDto
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.NodeScoreService
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallLight
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request
import kotlinx.coroutines.launch
import mu.KLogging

internal class RedisNodeScoreVerticle : CoroutineVerticle(), NodeScoreService {

    private companion object : KLogging()

    private var nodeAliveStateRefresherId: Long? = null
    private lateinit var options: Options
    private lateinit var redis: Redis
    private lateinit var nodeStateScoreSetName: String

    override suspend fun start() {
        options = config.mapTo(Options::class.java)
        redis = RedisHeimdallLight(vertx, options.redisOptions)
        nodeStateScoreSetName = "${options.clusterNodeId.clusterName}-node-scores"

        NodeScoreService.exposeService(vertx, this).await()

        setAndRefreshThisNodeAliveState()
        setThisNodeScore(0).await()
    }

    override suspend fun stop() {
        nodeAliveStateRefresherId?.let { vertx.cancelTimer(it) }
        CompositeFuture.all(removeNodeScoresOf(listOf(options.clusterNodeId)), removeThisNodeAliveState()).await()
    }

    override fun setThisNodeScore(score: Int): Future<Void> = redis.send(
        Request.cmd(Command.ZADD).arg(nodeStateScoreSetName).arg(score).arg("${options.clusterNodeId}")
    ).compose {
        logger.info { "Score of node ${options.clusterNodeId} updated to value $score" }
        Future.succeededFuture()
    }

    override fun getNodeScores(): Future<List<NodeScoreDto>> {
        val p = Promise.promise<List<NodeScoreDto>>()
        launch {
            p.complete(cleanAndGetScoresAliveNodes())
        }
        return p.future()
    }

    private fun setAndRefreshThisNodeAliveState() {
        val cmd = Request.cmd(Command.SET).arg("${options.clusterNodeId}").arg("1").arg("PX")
            .arg(options.nodeKeepAliveMillis)
        redis.send(cmd)
        nodeAliveStateRefresherId = vertx.setPeriodic(options.nodeKeepAliveMillis / 3) {
            redis.send(cmd)
        }
    }

    private fun removeThisNodeAliveState(): Future<Unit> =
        redis.send(Request.cmd(Command.DEL).arg("${options.clusterNodeId}")).compose { Future.succeededFuture() }

    private suspend fun cleanAndGetScoresAliveNodes(): List<NodeScoreDto> {
        val storedNodeScores = getStoredNodeScores()

        // Get indexes of missing node alive states according store node scores
        val notAliveNodeIndexes = redis.send(Request.cmd(Command.MGET).apply {
            storedNodeScores.forEach { score -> arg("${score.clusterNodeId}") }
        }).await().mapIndexedNotNull { idx, response ->
            if (response == null) idx else null
        }

        // Create a list of node ids for them a score exists, but no alive state is present
        val notAliveNodeIds = buildList {
            notAliveNodeIndexes.forEach { idx -> add(storedNodeScores[idx].clusterNodeId) }
        }

        // Remove all not alive node ids from store
        if (notAliveNodeIds.isNotEmpty()) {
            removeNodeScoresOf(notAliveNodeIds).await()
        }

        return storedNodeScores.filterNot { notAliveNodeIds.contains(it.clusterNodeId) }
    }

    private fun removeNodeScoresOf(nodeIds: List<OrchestraClusterNodeId>): Future<Unit> {
        return redis.send(Request.cmd(Command.ZREM).arg(nodeStateScoreSetName).apply {
            nodeIds.forEach { nodeId -> arg("$nodeId") }
        }).compose { Future.succeededFuture() }
    }

    private suspend fun getStoredNodeScores(): List<NodeScoreDto> {
        val nodeScores = ArrayList<NodeScoreDto>()
        val actualNodeScoresResponse =
            redis.send(Request.cmd(Command.ZRANGE).arg(nodeStateScoreSetName).arg(0).arg(-1).arg("WITHSCORES")).await()
        val actualNodeScoresIter = actualNodeScoresResponse.iterator()
        while (actualNodeScoresIter.hasNext()) {
            val nodeId = actualNodeScoresIter.next().toString()
            val score = actualNodeScoresIter.next().toInteger()
            nodeScores.add(NodeScoreDto(OrchestraClusterNodeId.of(nodeId), score))
        }
        return nodeScores
    }

    internal data class Options(
        val clusterNodeId: OrchestraClusterNodeId,
        val redisOptions: RedisHeimdallOptions,
        val nodeKeepAliveMillis: Long,
    )
}