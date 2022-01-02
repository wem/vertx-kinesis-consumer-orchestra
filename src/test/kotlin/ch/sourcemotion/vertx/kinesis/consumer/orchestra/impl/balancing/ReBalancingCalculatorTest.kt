package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.ints.shouldBeZero
import io.kotest.matchers.shouldBe
import org.junit.Test
import java.util.*

internal class ReBalancingCalculatorTest {

    @Test
    fun single_new_spawned_node_instance() {
        val nodeId = orchestraClusterNodeIdOf()
        val shardCountChange = 8
        val reBalancingCalcResult = ReBalancingCalculator(mapOf(nodeId to 0)).calculateReBalance(shardCountChange)
        val reBalancingEntry = reBalancingCalcResult.reBalancingNodeInfos.shouldHaveSize(1).first()
        reBalancingCalcResult.needsReBalancing().shouldBeTrue()
        reBalancingEntry.nodeId.shouldBe(nodeId)
        reBalancingEntry.adjustment.shouldBe(shardCountChange)
        reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
        reBalancingEntry.nodeShouldStartConsumers().shouldBeTrue()
        reBalancingEntry.noActionOnNode().shouldBeFalse()
    }

    @Test
    fun new_spawned_node_instance() {
        val existingNodeIds = listOf(orchestraClusterNodeIdOf(), orchestraClusterNodeIdOf(), orchestraClusterNodeIdOf())
        val newNodeId = orchestraClusterNodeIdOf()
        val reBalancingCalcResult = ReBalancingCalculator(
            mapOf(
                existingNodeIds[0] to 8,
                existingNodeIds[1] to 8,
                existingNodeIds[2] to 8,
                newNodeId to 0, // The new spawned node instance
            )
        ).calculateReBalance(0)

        reBalancingCalcResult.needsReBalancing().shouldBeTrue()
        reBalancingCalcResult.reBalancingNodeInfos.shouldHaveSize(4)
        var checked = 0
        reBalancingCalcResult.reBalancingNodeInfos.forEach { reBalancingEntry ->
            if (existingNodeIds.contains(reBalancingEntry.nodeId)) {
                reBalancingEntry.adjustment.shouldBe(-2)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeTrue()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeFalse()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
            if (reBalancingEntry.nodeId == newNodeId) {
                reBalancingEntry.adjustment.shouldBe(6)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeTrue()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
        }
        checked.shouldBe(4)
    }

    @Test
    fun node_instance_shutdown() {
        val reBalancingCalcResult = ReBalancingCalculator(
            mapOf(
                orchestraClusterNodeIdOf() to 6,
                orchestraClusterNodeIdOf() to 6,
                orchestraClusterNodeIdOf() to 6,
            )
        ).calculateReBalance(6)

        reBalancingCalcResult.needsReBalancing().shouldBeTrue()
        reBalancingCalcResult.reBalancingNodeInfos.shouldHaveSize(3)
        reBalancingCalcResult.reBalancingNodeInfos.forEach { reBalancingEntry ->
            reBalancingEntry.adjustment.shouldBe(2)
            reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
            reBalancingEntry.nodeShouldStartConsumers().shouldBeTrue()
            reBalancingEntry.noActionOnNode().shouldBeFalse()
        }
    }

    @Test
    fun event_shard_distribution() {
        val reBalancingCalcResult = ReBalancingCalculator(
            mapOf(
                orchestraClusterNodeIdOf() to 9,
                orchestraClusterNodeIdOf() to 9,
                orchestraClusterNodeIdOf() to 9,
                orchestraClusterNodeIdOf() to 9,
                orchestraClusterNodeIdOf() to 9,
                orchestraClusterNodeIdOf() to 9,
            )
        ).calculateReBalance(0)

        reBalancingCalcResult.needsReBalancing().shouldBeFalse()
        reBalancingCalcResult.reBalancingNodeInfos.shouldHaveSize(6)
        reBalancingCalcResult.reBalancingNodeInfos.forEach { reBalancingEntry ->
            reBalancingEntry.adjustment.shouldBeZero()
            reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
            reBalancingEntry.nodeShouldStartConsumers().shouldBeFalse()
            reBalancingEntry.noActionOnNode().shouldBeTrue()
        }
    }

    @Test
    fun uneven_distribution() {
        val nodeWithEightConsumers = orchestraClusterNodeIdOf()
        val nodeWithSixConsumers = orchestraClusterNodeIdOf()
        val nodeWithFourConsumers = orchestraClusterNodeIdOf()
        val nodeWithTwoConsumers = orchestraClusterNodeIdOf()
        val reBalancingCalcResult = ReBalancingCalculator(
            mapOf(
                nodeWithEightConsumers to 8,
                nodeWithSixConsumers to 6,
                nodeWithFourConsumers to 4,
                nodeWithTwoConsumers to 2,
            )
        ).calculateReBalance(0)

        var checked = 0
        reBalancingCalcResult.needsReBalancing().shouldBeTrue()
        reBalancingCalcResult.reBalancingNodeInfos.shouldHaveSize(4)
        reBalancingCalcResult.reBalancingNodeInfos.forEach { reBalancingEntry ->
            if (reBalancingEntry.nodeId == nodeWithEightConsumers) {
                reBalancingEntry.adjustment.shouldBe(-3)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeTrue()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeFalse()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
            if (reBalancingEntry.nodeId == nodeWithSixConsumers) {
                reBalancingEntry.adjustment.shouldBe(-1)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeTrue()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeFalse()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
            if (reBalancingEntry.nodeId == nodeWithFourConsumers) {
                reBalancingEntry.adjustment.shouldBe(1)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeTrue()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
            if (reBalancingEntry.nodeId == nodeWithTwoConsumers) {
                reBalancingEntry.adjustment.shouldBe(3)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeTrue()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
        }
        checked.shouldBe(4)
    }

    @Test
    fun uneven_distribution_and_consumable_shards() {
        val nodeWithEightConsumers = orchestraClusterNodeIdOf()
        val nodeWithSixConsumers = orchestraClusterNodeIdOf()
        val nodeWithFourConsumers = orchestraClusterNodeIdOf()
        val nodeWithTwoConsumers = orchestraClusterNodeIdOf()
        val reBalancingCalcResult = ReBalancingCalculator(
            mapOf(
                nodeWithEightConsumers to 8,
                nodeWithSixConsumers to 6,
                nodeWithFourConsumers to 4,
                nodeWithTwoConsumers to 2,
            )
        ).calculateReBalance(4)

        var checked = 0
        reBalancingCalcResult.needsReBalancing().shouldBeTrue()
        reBalancingCalcResult.reBalancingNodeInfos.shouldHaveSize(4)
        reBalancingCalcResult.reBalancingNodeInfos.forEach { reBalancingEntry ->
            if (reBalancingEntry.nodeId == nodeWithEightConsumers) {
                reBalancingEntry.adjustment.shouldBe(-2)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeTrue()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeFalse()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
            if (reBalancingEntry.nodeId == nodeWithSixConsumers) {
                reBalancingEntry.adjustment.shouldBe(0)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeFalse()
                reBalancingEntry.noActionOnNode().shouldBeTrue()
                checked++
            }
            if (reBalancingEntry.nodeId == nodeWithFourConsumers) {
                reBalancingEntry.adjustment.shouldBe(2)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeTrue()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
            if (reBalancingEntry.nodeId == nodeWithTwoConsumers) {
                reBalancingEntry.adjustment.shouldBe(4)
                reBalancingEntry.nodeShouldStopConsumers().shouldBeFalse()
                reBalancingEntry.nodeShouldStartConsumers().shouldBeTrue()
                reBalancingEntry.noActionOnNode().shouldBeFalse()
                checked++
            }
        }
        checked.shouldBe(4)
    }

    private fun orchestraClusterNodeIdOf(nodeId: String = "${UUID.randomUUID()}") =
        OrchestraClusterNodeId("cluster-name", nodeId)
}