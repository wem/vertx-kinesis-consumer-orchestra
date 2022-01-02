package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cluster.RedisNodeScoreVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.matchers.collections.*
import io.kotest.matchers.shouldBe
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Request
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.*

internal class BalancingVerticleTest : AbstractRedisTest() {

    private companion object {
        const val DEFAULT_ACTIVE_BALANCER_CHECK_INTERVAL_MILLIS = 1000L
        const val DEFAULT_INITIAL_BALANCING_DELAY_MILLIS = 1000L
        const val DEFAULT_BALANCING_INTERVAL_MILLIS = 1000L
        const val DEFAULT_BALANCING_COMMAND_TIMEOUT_MILLIS = 1000L
        const val DEFAULT_NODE_KEEP_ALIVE_MILLIS = 1000L
        const val CLUSTER_NAME = "test-cluster"
    }

    private val additionalVertxInstances = ArrayList<Vertx>()

    @AfterEach
    internal fun closeAdditionalVertxInstances() = asyncBeforeOrAfter {
        CompositeFuture.all(additionalVertxInstances.map { it.close() }).await()
    }

    @Test
    internal fun active_passive_balancer_rollover(testContext: VertxTestContext) = testContext.async {
        val activeOptions = verticleOptionsOf()
        val passiveOptions = verticleOptionsOf()

        ConsumerControlService.exposeService(vertx, NoopConsumerControlService)
        ConsumableShardDetectionService.exposeService(vertx, NoopConsumableShardDetectionService)

        deployNodeStateService(activeOptions.clusterNodeId)
        deployNodeStateService(passiveOptions.clusterNodeId)

        val activeDeploymentId = deployBalancingVerticle(activeOptions)
        delay(1000)
        deployBalancingVerticle(passiveOptions)

        repeat(5) {
            verifyActiveNode(activeOptions.clusterNodeId)
            delay(DEFAULT_ACTIVE_BALANCER_CHECK_INTERVAL_MILLIS)
        }

        // Undeploy active balancer node, so passive will become active
        vertx.undeploy(activeDeploymentId).await()

        // Give passive enough time for rollover to active before we verify
        delay(DEFAULT_ACTIVE_BALANCER_CHECK_INTERVAL_MILLIS * 2)

        repeat(5) {
            verifyActiveNode(passiveOptions.clusterNodeId)
            delay(DEFAULT_ACTIVE_BALANCER_CHECK_INTERVAL_MILLIS)
        }
    }

    @Test
    internal fun re_balancing_single_node_start_consumers(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val balancerOptions = verticleOptionsOf()
            val expectedShardIdsToStart = listOf(ShardId("1"), ShardId("2"), ShardId("3"))
            val startConsumerCheckpoint = testContext.checkpoint()
            val detectConsumableShardsCheckpoint = testContext.checkpoint()

            deployNodeStateService(balancerOptions.clusterNodeId)
            ConsumerControlService.exposeService(vertx, object : ConsumerControlService {
                override fun stopConsumers(consumerCount: Int): Future<StopConsumersCmdResult> {
                    val msg = "Stop consumers is not expected"
                    testContext.failNow(Exception(msg))
                    return Future.failedFuture(msg)
                }

                override fun startConsumers(shardIds: List<ShardId>): Future<Int> {
                    shardIds.shouldContainExactly(expectedShardIdsToStart)
                    startConsumerCheckpoint.flag()
                    return Future.succeededFuture(shardIds.size)
                }
            })
            ConsumableShardDetectionService.exposeService(vertx, object : ConsumableShardDetectionService {
                override fun getConsumableShards(): Future<List<ShardId>> {
                    detectConsumableShardsCheckpoint.flag()
                    return Future.succeededFuture(expectedShardIdsToStart)
                }
            })

            deployBalancingVerticle(balancerOptions)
            checkpoint.flag()
        }

    @Test
    internal fun re_balancing_additional_node_delayed(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val firstNodeBalancerOptions = verticleOptionsOf()
            val allShards = IntRange(0, 3).map { ShardId("$it") }
            val shardsToBalance = allShards.take(2)

            val startConsumersCheckpoint = testContext.checkpoint(2)
            val stopConsumersCheckpoint = testContext.checkpoint()
            val detectConsumableShardsCheckpoint = testContext.checkpoint()

            ConsumerControlService.exposeService(vertx, object : ConsumerControlService {
                private var consumerStarts = 0
                override fun stopConsumers(consumerCount: Int): Future<StopConsumersCmdResult> {
                    // Stop should get called one time, after the second node was coming up and the re-balancing happens
                    testContext.verify { consumerCount.shouldBe(shardsToBalance.size) }
                    stopConsumersCheckpoint.flag()
                    return Future.succeededFuture(
                        StopConsumersCmdResult(
                            shardsToBalance,
                            allShards.size - shardsToBalance.size
                        )
                    )
                }

                override fun startConsumers(shardIds: List<ShardId>): Future<Int> {
                    // Start consumers should happen 2 times. One time on each node
                    testContext.verify {
                        if (++consumerStarts == 1) {
                            shardIds.shouldContainExactly(allShards)
                            // Deploy second node right after first was balanced
                            defaultTestScope.launch {
                                val secondNodeBalancerOptions = verticleOptionsOf()
                                deployNodeStateService(secondNodeBalancerOptions.clusterNodeId)
                                deployBalancingVerticle(secondNodeBalancerOptions)
                            }
                        } else {
                            shardsToBalance.shouldContainExactly(shardsToBalance)
                        }
                    }
                    startConsumersCheckpoint.flag()
                    return Future.succeededFuture(shardIds.size)
                }
            })

            ConsumableShardDetectionService.exposeService(vertx, object : ConsumableShardDetectionService {
                private var detected = false
                override fun getConsumableShards(): Future<List<ShardId>> {
                    if (!detected) {
                        detected = true
                        detectConsumableShardsCheckpoint.flag()
                        return Future.succeededFuture(allShards)
                    }
                    return Future.succeededFuture(emptyList())
                }
            })

            deployNodeStateService(firstNodeBalancerOptions.clusterNodeId)
            deployBalancingVerticle(firstNodeBalancerOptions)

            checkpoint.flag()
        }

    @Test
    internal fun re_balancing_two_nodes_immediate(testContext: VertxTestContext) = testContext.async {
        val expectedNodeCount = 2

        val allShards = IntRange(0, 3).map { ShardId("$it") }

        val sharedConsumableShardDetection = SharedConsumableShardDetectionService(allShards)
        val nodeInstances = deployNodeInstances(expectedNodeCount, CLUSTER_NAME, sharedConsumableShardDetection)

        val nodeScoreService = NodeScoreService.createService(additionalVertxInstances.last())

        delay(5000)
        val nodeScores = nodeScoreService.getNodeScores().await()
        nodeScores.shouldHaveSize(expectedNodeCount)
        nodeScores.shouldContainExactlyInAnyOrder(nodeInstances.map { NodeScoreDto(it.nodeId, 2) })
    }

    @Test
    internal fun re_balancing_ten_nodes_even(testContext: VertxTestContext) = testContext.async {
        val expectedNodeCount = 10
        val allShards = IntRange(1, expectedNodeCount * 2).map { ShardId("$it") }

        val sharedConsumableShardDetection = SharedConsumableShardDetectionService(allShards)
        val nodeInstances = deployNodeInstances(expectedNodeCount, CLUSTER_NAME, sharedConsumableShardDetection)

        val nodeScoreService = NodeScoreService.createService(additionalVertxInstances.first())

        delay(5000)
        val nodeScores = nodeScoreService.getNodeScores().await()
        nodeScores.shouldHaveSize(expectedNodeCount)
        nodeScores.map { it.clusterNodeId }.shouldContainExactlyInAnyOrder(nodeInstances.map { it.nodeId })
        nodeScores.map { it.score }.shouldContain(2)
    }

    @Test
    internal fun re_balancing_ten_nodes_uneven(testContext: VertxTestContext) = testContext.async {
        val expectedNodeCount = 10
        val twoScoredNodesCheckpoint = testContext.checkpoint(8)
        val threeScoredNodesCheckpoint = testContext.checkpoint(8)

        val allShards = IntRange(1, expectedNodeCount * 2 + 2).map { ShardId("$it") }

        val sharedConsumableShardDetection = SharedConsumableShardDetectionService(allShards)
        val nodeInstances = deployNodeInstances(expectedNodeCount, CLUSTER_NAME, sharedConsumableShardDetection)

        val nodeScoreService = NodeScoreService.createService(additionalVertxInstances.first())

        delay(5000)
        val nodeScores = nodeScoreService.getNodeScores().await()
        nodeScores.shouldHaveSize(expectedNodeCount)
        nodeScores.map { it.clusterNodeId }.shouldContainExactlyInAnyOrder(nodeInstances.map { it.nodeId })
        nodeScores.map { it.score }.forEach { score ->
            if (score == 2) {
                twoScoredNodesCheckpoint.flag()
            }
            if (score == 3) {
                threeScoredNodesCheckpoint.flag()
            }
        }
    }

    @Test
    internal fun re_balancing_initial_ten_nodes_than_shutdown_five(testContext: VertxTestContext) = testContext.async {
        val clusterName = "test-cluster"
        val expectedNodeCount = 10

        // 20 shards
        val allShards = IntRange(1, expectedNodeCount * 2).map { ShardId("$it") }

        val sharedConsumableShardDetection = SharedConsumableShardDetectionService(allShards)
        val nodeInstances = deployNodeInstances(expectedNodeCount, clusterName, sharedConsumableShardDetection)

        delay(5000)

        val nodeScores = NodeScoreService.createService(additionalVertxInstances.last()).getNodeScores().await()
        nodeScores.shouldHaveSize(expectedNodeCount)
        nodeScores.map { it.clusterNodeId }.shouldContainExactlyInAnyOrder(nodeInstances.map { it.nodeId })

        // Close the half of Vert.x instances and summarize the no more consumed shards
        val noMoreConsumedShards = nodeInstances.takeAndRemove(5).map {
            it.vertxInstance.close().await()
            it.consumerControlService
        }.map { it.activeConsumedShards }.flatten()

        sharedConsumableShardDetection.noMoreConsumed(noMoreConsumedShards)

        delay(5000)

        NodeScoreService.createService(additionalVertxInstances.last()).getNodeScores().await().map { it.score }.shouldContain(4)
    }

    @Test
    internal fun re_balancing_redeployment_scenario(testContext: VertxTestContext) = testContext.async {
        val clusterName = "test-cluster"
        val expectedNodeCount = 10

        // 20 shards
        val allShards = IntRange(1, expectedNodeCount * 2).map { ShardId("$it") }

        val sharedConsumableShardDetection = SharedConsumableShardDetectionService(allShards)
        // Initial deployment
        val nodeInstances = deployNodeInstances(expectedNodeCount, clusterName, sharedConsumableShardDetection)
        val initialNodeIds = nodeInstances.map { it.nodeId }

        delay(5000)

        val nodeScores = NodeScoreService.createService(additionalVertxInstances.last()).getNodeScores().await()
        nodeScores.shouldHaveSize(expectedNodeCount)
        nodeScores.map { it.clusterNodeId }.shouldContainExactlyInAnyOrder(initialNodeIds)

        val redeployedNodeInstances = ArrayList<TestNodeInstanceInfo>()

        val nodeCountRedeploymentCycle = 2
        // We replace each node instance
        println("Redeployment started")
        repeat(5) {
            println("Will shutdown $nodeCountRedeploymentCycle node instances")
            val noMoreConsumedShards = nodeInstances.takeAndRemove(nodeCountRedeploymentCycle).map {
                it.vertxInstance.close().await()
                it.consumerControlService
            }.map { it.activeConsumedShards }.flatten()
            println("$nodeCountRedeploymentCycle node instances shutdown")
            sharedConsumableShardDetection.noMoreConsumed(noMoreConsumedShards)
            redeployedNodeInstances.addAll(deployNodeInstances(nodeCountRedeploymentCycle, clusterName, sharedConsumableShardDetection))
            println("Redeployed $nodeCountRedeploymentCycle node instances")
        }
        println("Redeployed should be done done")

        delay(5000)

        redeployedNodeInstances.shouldHaveSize(expectedNodeCount)
        redeployedNodeInstances.map { it.nodeId }.shouldNotContainAnyOf(initialNodeIds) // Verify all instances are replaced
        NodeScoreService.createService(additionalVertxInstances.last()).getNodeScores().await().map { it.score }.shouldContain(2)
    }

    private fun startVertxInstances(expectedNodeCount: Int): List<Vertx> {
        return IntRange(1, expectedNodeCount).map { Vertx.vertx().also { additionalVertxInstances.add(it) } }
    }

    private suspend fun deployNodeInstances(
        expectedInstances: Int,
        clusterName: String,
        sharedConsumableShardDetection: SharedConsumableShardDetectionService
    ) = (startVertxInstances(expectedInstances)).mapIndexed { idx, additionalVertx ->
        val nodeId = OrchestraClusterNodeId(clusterName, "${UUID.randomUUID()}")
        val verticleOptions = verticleOptionsOf(nodeId)

        deployNodeStateService(nodeId, additionalVertx)

        val consumerControlService = TestConsumerControlService(sharedConsumableShardDetection)
        ConsumerControlService.exposeService(
            additionalVertx,
            consumerControlService
        )

        ConsumableShardDetectionService.exposeService(
            additionalVertx,
            sharedConsumableShardDetection
        )

        deployBalancingVerticle(verticleOptions, additionalVertx)
        TestNodeInstanceInfo(nodeId, additionalVertx, consumerControlService)
    }.toMutableList()

    private suspend fun deployBalancingVerticle(options: BalancingVerticle.Options, vertx: Vertx = this.vertx): String {
        return vertx.deployVerticle(BalancingVerticle::class.java, options.toDeploymentOptions()).await()
    }

    private suspend fun deployNodeStateService(
        nodeId: OrchestraClusterNodeId,
        vertx: Vertx = this.vertx,
        nodeKeepAliveMillis: Long = DEFAULT_NODE_KEEP_ALIVE_MILLIS,
    ): String {
        val options = RedisNodeScoreVerticle.Options(nodeId, redisHeimdallOptions, nodeKeepAliveMillis)
        return vertx.deployVerticle(
            RedisNodeScoreVerticle::class.java,
            deploymentOptionsOf(config = JsonObject.mapFrom(options))
        ).await()
    }

    private suspend fun verifyActiveNode(nodeId: OrchestraClusterNodeId) {
        redisClient.send(Request.cmd(Command.GET).arg("${nodeId.clusterName}-balancer")).await()
            .toString().shouldBe("$nodeId")
    }

    private fun verticleOptionsOf(
        nodeId: OrchestraClusterNodeId = OrchestraClusterNodeId("test-cluster", "${UUID.randomUUID()}"),
        activeBalancerCheckIntervalMillis: Long = DEFAULT_ACTIVE_BALANCER_CHECK_INTERVAL_MILLIS,
        initialBalancingDelayMillis: Long = DEFAULT_INITIAL_BALANCING_DELAY_MILLIS,
        balancingIntervalMillis: Long = DEFAULT_BALANCING_INTERVAL_MILLIS,
        balancingCommandTimeoutMillis: Long = DEFAULT_BALANCING_COMMAND_TIMEOUT_MILLIS,
    ) = BalancingVerticle.Options(
        nodeId,
        redisHeimdallOptions,
        activeBalancerCheckIntervalMillis,
        initialBalancingDelayMillis,
        balancingIntervalMillis,
        balancingCommandTimeoutMillis
    )
}

private data class TestNodeInstanceInfo(
    val nodeId: OrchestraClusterNodeId,
    val vertxInstance: Vertx,
    val consumerControlService: TestConsumerControlService
)

private class SharedConsumableShardDetectionService(allShardIds: List<ShardId>) : ConsumableShardDetectionService {
    private val consumableShards = ArrayList(allShardIds)

    override fun getConsumableShards(): Future<List<ShardId>> = Future.succeededFuture(ArrayList(consumableShards))

    @Synchronized
    fun nowConsumed(shardIds: List<ShardId>) {
        consumableShards.removeAll(shardIds.toSet())
    }

    @Synchronized
    fun noMoreConsumed(shardIds: List<ShardId>) {
        consumableShards.addAll(shardIds)
    }
}

private class TestConsumerControlService(
    private val consumableDetection: SharedConsumableShardDetectionService
) : ConsumerControlService {
    val activeConsumedShards = ArrayList<ShardId>()

    override fun stopConsumers(consumerCount: Int): Future<StopConsumersCmdResult> {
        val stoppedShardIds = activeConsumedShards.takeAndRemove(consumerCount)
        consumableDetection.noMoreConsumed(stoppedShardIds)
        return Future.succeededFuture(StopConsumersCmdResult(stoppedShardIds, activeConsumedShards.size))
    }

    override fun startConsumers(shardIds: List<ShardId>): Future<Int> {
        activeConsumedShards.addAll(shardIds)
        consumableDetection.nowConsumed(shardIds)
        return Future.succeededFuture(activeConsumedShards.size)
    }
}


private class DetectOnceConsumableShardDetectionService(shardsToDetect: List<ShardId>) :
    ConsumableShardDetectionService {
    private val pendingShardsToDetect = shardsToDetect.toMutableList()
    override fun getConsumableShards(): Future<List<ShardId>> {
        // We detect all shards at once at consumable
        val detectedShards = pendingShardsToDetect.takeAndRemove(pendingShardsToDetect.size)
        return Future.succeededFuture(detectedShards)
    }
}


private object NoopConsumerControlService : ConsumerControlService {
    override fun stopConsumers(consumerCount: Int): Future<StopConsumersCmdResult> = Future.succeededFuture(
        StopConsumersCmdResult(
            emptyList(), 1
        )
    )

    override fun startConsumers(shardIds: List<ShardId>): Future<Int> = Future.succeededFuture(0)
}

private object NoopConsumableShardDetectionService : ConsumableShardDetectionService {
    override fun getConsumableShards(): Future<List<ShardId>> = Future.succeededFuture(emptyList())
}

internal fun BalancingVerticle.Options.toDeploymentOptions() = deploymentOptionsOf(config = JsonObject.mapFrom(this))