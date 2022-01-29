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
import io.vertx.redis.client.impl.types.MultiType
import kotlinx.coroutines.launch
import mu.KLogging

internal class RedisNodeScoreVerticle : CoroutineVerticle(), NodeScoreService {

    private companion object : KLogging()

    private var nodeAliveStateRefresherId: Long? = null
    private lateinit var options: Options
    private lateinit var redis: Redis
    private lateinit var nodeStateScoreSetName: String

    // If ths update of this nodes score did fail, we will retry later, beside keep alive
    private var thisScoreDrift: Int? = null

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
        runCatching {
            CompositeFuture.all(removeNodeScoresOf(listOf(options.clusterNodeId)), removeThisNodeAliveState()).await()
        }
    }

    override fun setThisNodeScore(score: Int): Future<Void> {
        thisScoreDrift = null
        return redis.send(
            Request.cmd(Command.ZADD).arg(nodeStateScoreSetName).arg(score).arg("${options.clusterNodeId}")
        ).onFailure { cause ->
            thisScoreDrift = score
            logger.warn(cause) { "Failed to set score of node ${options.clusterNodeId}" }
        }.compose {
            logger.info { "Score of node ${options.clusterNodeId} updated to $score" }
            Future.succeededFuture()
        }
    }

    override fun getNodeScores(): Future<List<NodeScoreDto>> {
        val p = Promise.promise<List<NodeScoreDto>>()
        launch {
            try {
                p.complete(cleanAndGetScoresAliveNodes())
            } catch (e: Exception) {
                logger.warn(e) { "Failed to get node scores" }
                p.fail(e)
            }
        }
        return p.future()
    }

    private fun setAndRefreshThisNodeAliveState() {
        val cmd = Request.cmd(Command.SET).arg("${options.clusterNodeId}").arg("1").arg("PX")
            .arg(options.nodeKeepAliveMillis)
        redis.send(cmd)
        nodeAliveStateRefresherId = vertx.setPeriodic(options.nodeKeepAliveMillis / 3) {
            redis.send(cmd)
                .onFailure { cause -> logger.warn(cause) { "Failed to set score alive state of node ${options.clusterNodeId}" } }
            // Self-healing if the previous score set command of this node did fail
            val scoreDrift = thisScoreDrift
            if (scoreDrift != null) {
                setThisNodeScore(scoreDrift)
            }
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
            logger.info { "Remove score(s) of node(s): $notAliveNodeIds" }
            removeNodeScoresOf(notAliveNodeIds).await()
        }

        return storedNodeScores.filterNot { storedNodeScore -> notAliveNodeIds.contains(storedNodeScore.clusterNodeId) }
    }

    private fun removeNodeScoresOf(nodeIds: List<OrchestraClusterNodeId>): Future<Unit> {
        return redis.send(Request.cmd(Command.ZREM).arg(nodeStateScoreSetName).apply {
            nodeIds.forEach { nodeId -> arg("$nodeId") }
        }).compose { Future.succeededFuture() }
    }

    private suspend fun getStoredNodeScores(): List<NodeScoreDto> {
        val nodeScores = ArrayList<NodeScoreDto>()
        val nodeScoresResponse =
            redis.send(Request.cmd(Command.ZRANGE).arg(nodeStateScoreSetName).arg(0).arg(-1).arg("WITHSCORES")).await()
        val nodeScoresResponseIter = nodeScoresResponse.iterator()
        while (nodeScoresResponseIter.hasNext()) {
            val leadEntry = nodeScoresResponseIter.next()
            // Redis 6 support
            if (leadEntry is MultiType) {
                val nodeId = leadEntry.first().toString()
                val score = leadEntry.last().toDouble().toInt()
                nodeScores.add(NodeScoreDto(OrchestraClusterNodeId.of(nodeId), score))
            // Redis 5 support
            } else {
                val nodeId = leadEntry.toString()
                val score = nodeScoresResponseIter.next().toDouble().toInt()
                nodeScores.add(NodeScoreDto(OrchestraClusterNodeId.of(nodeId), score))
            }
        }
        return nodeScores
    }

    internal data class Options(
        val clusterNodeId: OrchestraClusterNodeId,
        val redisOptions: RedisHeimdallOptions,
        val nodeKeepAliveMillis: Long,
    )
}