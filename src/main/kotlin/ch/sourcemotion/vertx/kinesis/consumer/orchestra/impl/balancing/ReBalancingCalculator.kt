package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId

/**
 * Calculator for shard (re)-balancing between nodes. The calculation results is as even as possible.
 */
class ReBalancingCalculator(private val consumedShardsByNode: List<Pair<OrchestraClusterNodeId, Int>>) {
    fun calculateReBalance(shardCountChange: Int): ReBalancingCalcResult {
        val overallShardCount = consumedShardsByNode.sumOf { it.second } + shardCountChange

        // Ensure list starts with the highest value
        val consumedShardsByNodeSorted = consumedShardsByNode.sortedByDescending { it.second }
        val shardDistribution = ArrayList<Pair<OrchestraClusterNodeId, Counter>>(consumedShardsByNodeSorted.map { it.first to Counter() })
        var shardDistributionIter = shardDistribution.iterator()
        repeat(overallShardCount) {
            if (!shardDistributionIter.hasNext()) {
                shardDistributionIter = shardDistribution.iterator()
            }
            shardDistributionIter.next().second.incr()
        }

        val reBalancingEntries = shardDistribution.associate {
            it.first to it.second.value() - consumedShardsByNode.first { original -> original.first == it.first }.second
        }.map { ReBalancingNodeInfo(it.key, it.value) }

        return ReBalancingCalcResult(reBalancingEntries)
    }
}

private class Counter(private var value: Int = 0) {
    fun incr() {
        ++value
    }
    fun value() = value
}


data class ReBalancingCalcResult(val reBalancingNodeInfos: List<ReBalancingNodeInfo>) {
    fun needsReBalancing() = reBalancingNodeInfos.map { !it.noActionOnNode() }.any { it }
}

data class ReBalancingNodeInfo(val nodeId: OrchestraClusterNodeId, val adjustment: Int) {
    fun nodeShouldStopConsumers() = adjustment < 0
    fun nodeShouldStartConsumers() = adjustment > 0
    fun noActionOnNode() = adjustment == 0
}