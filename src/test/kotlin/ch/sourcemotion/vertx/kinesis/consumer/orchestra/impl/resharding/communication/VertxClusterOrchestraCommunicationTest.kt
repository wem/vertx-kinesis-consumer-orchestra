package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication.OrchestraCommunicationTestDefinition.Companion.clusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication.OrchestraCommunicationTestDefinition.Companion.shardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test

internal class VertxClusterOrchestraCommunicationTest : OrchestraCommunicationTestDefinition, AbstractVertxTest() {

    @Test
    override fun common_workflow(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val commandSender = VertxClusterOrchestraCommunication(vertx, clusterName) {
            testContext.failNow(Exception("initiator consume shard command handler should not get called"))
        }

        val commandReceiver = VertxClusterOrchestraCommunication(vertx, clusterName) {
            testContext.verify { it.shouldBe(shardId) }
            checkpoint.flag()
        }

        // Command receiver not ready to receive command broadcasts
        commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()

        commandReceiver.readyForShardConsumeCommands()
        commandSender.trySendShardConsumeCmd(shardId).shouldBeTrue()
    }

    @Test
    override fun command_receiver_fail(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val commandSender = VertxClusterOrchestraCommunication(vertx, clusterName) {
            testContext.failNow(Exception("initiator consume shard command handler should not get called"))
        }

        val commandReceiver = VertxClusterOrchestraCommunication(vertx, clusterName) {
            testContext.verify { it.shouldBe(shardId) }
            checkpoint.flag()
            throw Exception("Test exception on Vert.x remote command receiver")
        }

        // Command receiver not ready to receive command broadcasts
        commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()

        commandReceiver.readyForShardConsumeCommands()
        commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()
    }

    @Test
    override fun command_to_ifself(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val commandSenderAndReceiver = VertxClusterOrchestraCommunication(vertx, clusterName) {
            testContext.verify { it.shouldBe(shardId) }
            checkpoint.flag()
        }

        // Command receiver not ready to receive command broadcasts
        commandSenderAndReceiver.trySendShardConsumeCmd(shardId).shouldBeFalse()

        commandSenderAndReceiver.readyForShardConsumeCommands()
        commandSenderAndReceiver.trySendShardConsumeCmd(shardId).shouldBeTrue()
    }

    @Test
    override fun multiple_potential_receivers(testContext: VertxTestContext) =
        testContext.asyncDelayed(1, 200) { checkpoint ->
            val commandSender = VertxClusterOrchestraCommunication(vertx, clusterName) {
                testContext.failNow(Exception("initiator consume shard command handler should not get called"))
            }

            // Only one receiver should receive the command
            val commandReceivers = listOf(
                VertxClusterOrchestraCommunication(vertx, clusterName) {
                    testContext.verify { it.shouldBe(shardId) }
                    checkpoint.flag()
                },
                VertxClusterOrchestraCommunication(vertx, clusterName) {
                    testContext.verify { it.shouldBe(shardId) }
                    checkpoint.flag()
                }
            )

            // Command receiver not accepts command broadcasts
            commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()

            commandReceivers.forEach { it.readyForShardConsumeCommands() }
            commandSender.trySendShardConsumeCmd(shardId).shouldBeTrue()
        }

    @Test
    override fun no_command_received_if_not_ready(testContext: VertxTestContext) =
        testContext.asyncDelayed(1, 200) { checkpoint ->
            val commandSender = VertxClusterOrchestraCommunication(vertx, clusterName) {
                testContext.failNow(Exception("initiator consume shard command handler should not get called"))
            }

            val commandReceiver = VertxClusterOrchestraCommunication(vertx, clusterName) {
                testContext.verify { it.shouldBe(shardId) }
                checkpoint.flag()
            }

            commandReceiver.readyForShardConsumeCommands()
            commandSender.trySendShardConsumeCmd(shardId).shouldBeTrue()
            commandReceiver.notReadyForShardConsumeCommands()
            commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()
        }
}
