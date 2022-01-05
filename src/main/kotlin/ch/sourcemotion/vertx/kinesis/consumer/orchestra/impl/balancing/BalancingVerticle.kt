package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.ConsumableShardDetectionService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.ConsumerControlService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.NodeScoreService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.StopConsumersCmdResult
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallLight
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Request
import kotlinx.coroutines.launch
import mu.KLogging

internal class BalancingVerticle : CoroutineVerticle() {

    private companion object : KLogging()

    private var activeBalancer = false
    private var activeBalancerRefreshTimerId: Long? = null
    private var balancingTimerId: Long? = null

    private lateinit var options: Options
    private lateinit var redis: RedisHeimdallLight
    private lateinit var balancingNodeCommunication: BalancingNodeCommunication
    private lateinit var nodeScoreService: NodeScoreService
    private lateinit var consumerControlService: ConsumerControlService
    private lateinit var consumableShardDetectionService: ConsumableShardDetectionService

    private var reBalancingInProgress = false
    private var firstConsumerStart = true

    override suspend fun start() {
        options = config.mapTo(Options::class.java)
        redis = RedisHeimdallLight(vertx, options.redisOptions)
        balancingNodeCommunication = RedisBalancingNodeCommunication(
            vertx,
            options.redisOptions,
            options.clusterNodeId,
            options.balancingCommandTimeoutMillis,
            ::onStopConsumerBalancingCmd,
            ::onStartConsumerBalancingCmd
        ).start()

        nodeScoreService = NodeScoreService.createService(vertx)
        consumerControlService = ConsumerControlService.createService(vertx)
        consumableShardDetectionService = ConsumableShardDetectionService.createService(vertx)

        activeBalancerHandling()
        logger.info { "Balancer of node ${options.clusterNodeId} started" }
    }

    override suspend fun stop() {
        balancingTimerId?.let { vertx.cancelTimer(it) }
        activeBalancerRefreshTimerId?.let { vertx.cancelTimer(it) }
        releaseThisActiveBalancer().await()
        logger.info { "Balancer of node ${options.clusterNodeId} stopped" }
    }

    private fun onStopConsumerBalancingCmd(consumerCount: Int): Future<StopConsumersCmdResult> {
        return consumerControlService.stopConsumers(consumerCount)
    }

    private fun onStartConsumerBalancingCmd(shardIds: List<ShardId>): Future<Int> {
        return consumerControlService.startConsumers(shardIds)
    }

    private fun activeBalancerHandling() {
        activeBalancerRefreshTimerId = vertx.setPeriodic(options.activeBalancerCheckIntervalMillis) {
            tryBecomeOrKeepActiveBalancer(activeBalancer).onSuccess { isNowActiveBalancer ->
                // Became new active?
                if (!activeBalancer && isNowActiveBalancer) {
                    logger.info { "Became active balancer: ${options.clusterNodeId} on cluster ${options.clusterNodeId.clusterName}" }
                    // If we became active, we will wait for a while before we start balancing, so we give
                    // Other node a bit time for bootstrap
                    vertx.setTimer(options.initialBalancingDelayMillis) {
                        balancingTimerId = vertx.setPeriodic(options.balancingIntervalMillis, ::executeBalancing)
                    }
                }
                if (activeBalancer && !isNowActiveBalancer) {
                    logger.warn { "Lost active balancer state. Will stop balancing: ${options.clusterNodeId} on cluster ${options.clusterNodeId.clusterName}" }
                    balancingTimerId?.let { vertx.cancelTimer(it) }
                }
                activeBalancer = isNowActiveBalancer
            }
        }
    }

    private fun executeBalancing(@Suppress("UNUSED_PARAMETER") timerId: Long) {
        if (reBalancingInProgress || !activeBalancer) return
        launch {
            reBalancingInProgress = true
            val nodeScores = nodeScoreService.getNodeScores().await()
            val consumableShardIds = consumableShardDetectionService.getConsumableShards().await()
            val reBalancingCalcResult =
                ReBalancingCalculator(nodeScores.map { it.clusterNodeId to it.score }).calculateReBalance(
                    consumableShardIds.size
                )

            if (reBalancingCalcResult.needsReBalancing()) {
                logger.info { "Initiate re-balancing because of node scores $nodeScores and / or consumable shards $consumableShardIds on cluster ${options.clusterNodeId.clusterName}" }
                // Filter nodes their score is enough high so on them no re-balancing will happen
                val pendingReBalancingNodeInfos =
                    reBalancingCalcResult.reBalancingNodeInfos.filterNot { it.noActionOnNode() }

                val nodesToStopConsumersOn = pendingReBalancingNodeInfos.filter { it.nodeShouldStopConsumers() }
                val pendingConsumerStopJobs = stopConsumersOnNodes(nodesToStopConsumersOn)

                // Available shards are the stopped ones on nodes and the additional, consumable
                val stoppedShardIds = pendingConsumerStopJobs.map { it.await() }.map { it.stoppedShardIds }.flatten()
                if (stoppedShardIds.isNotEmpty()) {
                    logger.info { "Stopped shards $stoppedShardIds during re-balancing on cluster ${options.clusterNodeId.clusterName}" }
                }
                val availableShardIds = (stoppedShardIds + consumableShardIds).toMutableList()

                val nodesToStartConsumersOn = pendingReBalancingNodeInfos.filter { it.nodeShouldStartConsumers() }
                val pendingConsumerStartJobs =
                    startConsumersOnNodes(nodesToStartConsumersOn, availableShardIds)
                CompositeFuture.all(pendingConsumerStartJobs).await()

                if (availableShardIds.isNotEmpty()) {
                    logger.warn { "There are available shards left after re-balancing $availableShardIds on cluster ${options.clusterNodeId.clusterName}" }
                }
                logger.info { "Re-balancing done on cluster ${options.clusterNodeId.clusterName}" }
            }
        }.invokeOnCompletion { failure ->
            reBalancingInProgress = false
            if (failure != null) {
                logger.warn(failure) { "Re-balancing failed" }
            }
        }
    }

    private suspend fun startConsumersOnNodes(
        nodesToStartConsumersOn: List<ReBalancingNodeInfo>,
        availableShardIds: MutableList<ShardId>
    ) = nodesToStartConsumersOn.mapIndexed { idx, reBalancingNodeInfo ->
        val shardIdsToStart = availableShardIds.takeAndRemove(reBalancingNodeInfo.adjustment)
        logger.info { "Will start consumers for $shardIdsToStart on node ${reBalancingNodeInfo.nodeId} for re-balancing on cluster ${options.clusterNodeId.clusterName}" }

        // This node instance?
        val future = if (reBalancingNodeInfo.nodeId == options.clusterNodeId) {
            // We should not call consumer control directly, so node state get also updated
            onStartConsumerBalancingCmd(shardIdsToStart)
        } else {
            balancingNodeCommunication.sendStartConsumersCmd(reBalancingNodeInfo.nodeId, shardIdsToStart)
        }
        // If it's the first balancing where consumer are started, we wait until the first node is done.
        // This because otherwise and in the case of the enhanced fan out parallel consumer registration could happen
        // which could probably fail
        if (firstConsumerStart && idx == 0) {
            future.await()
            firstConsumerStart = false
        }
        future
    }

    private fun stopConsumersOnNodes(nodeToStopConsumersOn: List<ReBalancingNodeInfo>) =
        nodeToStopConsumersOn.map { reBalancingNodeInfo ->
            val consumerCountToStop =
                reBalancingNodeInfo.adjustment * -1 // Negative adjustment means consumer have to get stopped on that node
            logger.info { "Will stop $consumerCountToStop consumers on node ${reBalancingNodeInfo.nodeId} for re-balancing on cluster ${options.clusterNodeId.clusterName}" }
            // This node instance?
            if (reBalancingNodeInfo.nodeId == options.clusterNodeId) {
                // We should not call consumer control directly, so node state get also updated
                onStopConsumerBalancingCmd(consumerCountToStop)
            } else {
                balancingNodeCommunication.sendStopConsumersCmd(reBalancingNodeInfo.nodeId, consumerCountToStop)
            }
        }

    private fun tryBecomeOrKeepActiveBalancer(alreadyActiveBalancer: Boolean): Future<Boolean> {
        val cmd = Request.cmd(Command.SET).apply {
            arg("${options.clusterNodeId.clusterName}-balancer").arg("${options.clusterNodeId}")
                .arg("PX").arg(options.activeBalancerCheckIntervalMillis * 3)
            // If this node is not the active we set only if not set
            if (!alreadyActiveBalancer) {
                arg("NX")
            }
        }
        return redis.send(cmd).compose { Future.succeededFuture(it.okResponseAsBoolean()) }
    }

    private fun releaseThisActiveBalancer(): Future<Boolean> {
        val cmd = Request.cmd(Command.DEL).arg("${options.clusterNodeId.clusterName}-balancer")
        return redis.send(cmd).compose { Future.succeededFuture(it.toInteger() == 1) }
    }

    internal data class Options(
        val clusterNodeId: OrchestraClusterNodeId,
        val redisOptions: RedisHeimdallOptions,
        val activeBalancerCheckIntervalMillis: Long,
        val initialBalancingDelayMillis: Long,
        val balancingIntervalMillis: Long,
        val balancingCommandTimeoutMillis: Long,
    )
}

internal fun <T> MutableCollection<T>.takeAndRemove(elements: Int): List<T> {
    val taken = take(elements)
    removeAll(taken.toSet())
    return taken
}