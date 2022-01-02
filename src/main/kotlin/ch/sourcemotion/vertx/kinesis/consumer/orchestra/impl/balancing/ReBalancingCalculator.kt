package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId

/**
 * Calculator for shard (re)-balancing between nodes. The calculation results is as even as possible.
 */
class ReBalancingCalculator(private val consumedShardsByNode: Map<OrchestraClusterNodeId, Int>) {
    fun calculateReBalance(shardCountChange: Int): ReBalancingCalcResult {
        val overallShardCount = consumedShardsByNode.values.sum() + shardCountChange

        val shardDistribution = HashMap<OrchestraClusterNodeId, Int>(consumedShardsByNode.mapValues { 0 })
        var shardDistributionIter = shardDistribution.iterator()
        repeat(overallShardCount) {
            if (!shardDistributionIter.hasNext()) {
                shardDistributionIter = shardDistribution.iterator()
            }
            val nodeId = shardDistributionIter.next().key
            shardDistribution[nodeId] = shardDistribution.getValue(nodeId) + 1
        }

        val reBalancingEntries = shardDistribution.mapValues { entry ->
            entry.value - consumedShardsByNode.getValue(entry.key)
        }.map { ReBalancingNodeInfo(it.key, it.value) }

        return ReBalancingCalcResult(reBalancingEntries)
    }
}

data class ReBalancingCalcResult(val reBalancingNodeInfos: List<ReBalancingNodeInfo>) {
    fun needsReBalancing() = reBalancingNodeInfos.map { !it.noActionOnNode() }.any { it }
}

data class ReBalancingNodeInfo(val nodeId: OrchestraClusterNodeId, val adjustment: Int) {
    fun nodeShouldStopConsumers() = adjustment < 0
    fun nodeShouldStartConsumers() = adjustment > 0
    fun noActionOnNode() = adjustment == 0
}