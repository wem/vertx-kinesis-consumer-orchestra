package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cluster

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.NodeScoreDto
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.NodeScoreService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Request
import io.vertx.serviceproxy.ServiceException
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test
import java.util.*

internal class RedisNodeScoreVerticleTest : AbstractRedisTest() {

    private companion object {
        const val DEFAULT_NODE_KEEP_ALIVE_MILLIS = 100L
    }

    @Test
    internal fun this_node_score_on_deploy(testContext: VertxTestContext) = testContext.async {
        val (_, nodeId) = deployNodeScoreVerticle()
        val sut = NodeScoreService.createService(vertx)
        val nodeScores = sut.getNodeScores().await()
        val thisNodeScore = nodeScores.shouldHaveSize(1).first()
        thisNodeScore.score.shouldBe(0)
        thisNodeScore.clusterNodeId.shouldBe(nodeId)
    }

    @Test
    internal fun multiple_nodes_deployment(testContext: VertxTestContext) = testContext.async {
        // Deploy main node and create service
        val remoteNodeCount = 11
        val (_, mainNodeId) = deployNodeScoreVerticle()
        val sut = NodeScoreService.createService(vertx)

        // Deploy some remote nodes and safe their deployment id
        val otherNodeStatesDeploymentIds = buildList {
            repeat(remoteNodeCount) {
                add(deployNodeScoreVerticle().first)
            }
        }

        // Verify node states
        val allNodeScores = sut.getNodeScores().await()
        allNodeScores.shouldHaveSize(remoteNodeCount + 1).forEach {
            it.score.shouldBe(0)
        }
        allNodeScores.shouldContain(NodeScoreDto(mainNodeId, 0))

        // Remove remote notes
        otherNodeStatesDeploymentIds.forEach { vertx.undeploy(it).await() }

        // Verify node scores after other nodes are shutdown
        val nodeScores = sut.getNodeScores().await()
        val thisNodeScore = nodeScores.shouldHaveSize(1).first()
        thisNodeScore.score.shouldBe(0)
        thisNodeScore.clusterNodeId.shouldBe(mainNodeId)
    }

    @Test
    internal fun update_this_node_score(testContext: VertxTestContext) = testContext.async {
        val expectedScore = 10
        val (_, nodeId) = deployNodeScoreVerticle()
        val sut = NodeScoreService.createService(vertx)
        sut.getNodeScores().await().let { nodeScores ->
            val nodeScoreDto = nodeScores.shouldHaveSize(1).first()
            nodeScoreDto.score.shouldBe(0)
            nodeScoreDto.clusterNodeId.shouldBe(nodeId)
        }

        sut.setThisNodeScore(expectedScore).await()
        val nodeScores = sut.getNodeScores().await()
        val thisNodeScore = nodeScores.shouldHaveSize(1).first()
        thisNodeScore.score.shouldBe(expectedScore)
        thisNodeScore.clusterNodeId.shouldBe(nodeId)
    }

    @Test
    internal fun cleanup_unhealthy_node_scores_even_score(testContext: VertxTestContext) = testContext.async {
        val (_, nodeId) = deployNodeScoreVerticle()
        val sut = NodeScoreService.createService(vertx)
        val nodeScoreSetName = "${nodeId.clusterName}-node-scores"

        // We add a score of some nodes for them the keep alive state is missing
        repeat(10) {
            val otherNodeId = OrchestraClusterNodeId(nodeId.clusterName, "${UUID.randomUUID()}")
            redisClient.send(Request.cmd(Command.ZADD).arg(nodeScoreSetName).arg("0").arg("$otherNodeId"))
        }

        val nodeScore = sut.getNodeScores().await().shouldHaveSize(1).first()
        nodeScore.clusterNodeId.shouldBe(nodeId)
        nodeScore.score.shouldBe(0)
    }

    @Test
    internal fun cleanup_unhealthy_node_scores_uneven_score(testContext: VertxTestContext) = testContext.async {
        val (_, nodeId) = deployNodeScoreVerticle()
        val sut = NodeScoreService.createService(vertx)
        val nodeScoreSetName = "${nodeId.clusterName}-node-scores"

        // We add a score of some nodes for them the keep alive state is missing
        repeat(10) {
            val otherNodeId = OrchestraClusterNodeId(nodeId.clusterName, "${UUID.randomUUID()}")
            redisClient.send(Request.cmd(Command.ZADD).arg(nodeScoreSetName).arg("$it").arg("$otherNodeId"))
        }

        val nodeScore = sut.getNodeScores().await().shouldHaveSize(1).first()
        nodeScore.clusterNodeId.shouldBe(nodeId)
        nodeScore.score.shouldBe(0)
    }

    @Test
    internal fun self_healing_score(testContext: VertxTestContext) = testContext.async {
        val (_, nodeId) = deployNodeScoreVerticle()
        val expectedScore = 10
        val sut = NodeScoreService.createService(vertx)

        closeConnectionToRedis()
        delay(DEFAULT_NODE_KEEP_ALIVE_MILLIS * 3) // Ensure PX reached

        shouldThrow<ServiceException> { sut.getNodeScores().await() }
        shouldThrow<ServiceException> { sut.setThisNodeScore(expectedScore).await() }

        removeRedisToxies()
        delay((redisHeimdallOptions.reconnectInterval * 3) + DEFAULT_NODE_KEEP_ALIVE_MILLIS * 3) // Ensure reconnected

        val thisNodeScore = sut.getNodeScores().await().shouldHaveSize(1).first()
        thisNodeScore.score.shouldBe(expectedScore)
        thisNodeScore.clusterNodeId.shouldBe(nodeId)
    }

    private suspend fun deployNodeScoreVerticle(
        nodeId: OrchestraClusterNodeId = orchestraClusterNodeId(),
        nodeKeepAliveMillis: Long = DEFAULT_NODE_KEEP_ALIVE_MILLIS
    ): Pair<String, OrchestraClusterNodeId> {
        val options = RedisNodeScoreVerticle.Options(nodeId, redisHeimdallOptions, nodeKeepAliveMillis)
        val deploymentId = vertx.deployVerticle(
            RedisNodeScoreVerticle::class.java,
            deploymentOptionsOf(config = JsonObject.mapFrom(options))
        ).await()
        return deploymentId to nodeId
    }

    private fun orchestraClusterNodeId() = OrchestraClusterNodeId(
        OrchestraClusterName("OrchestraNodeStateVerticleTest", "some-stream"),
        "${UUID.randomUUID()}"
    )
}