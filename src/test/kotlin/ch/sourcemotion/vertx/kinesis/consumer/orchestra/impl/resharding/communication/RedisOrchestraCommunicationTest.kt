package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication.OrchestraCommunicationTestDefinition.Companion.clusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication.OrchestraCommunicationTestDefinition.Companion.shardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test

internal class RedisOrchestraCommunicationTest : OrchestraCommunicationTestDefinition, AbstractRedisTest() {
    @Test
    override fun common_workflow(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val commandSender = RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
            testContext.failNow(Exception("initiator consume shard command handler should not get called"))
        }.start()

        val commandReceiver = RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
            testContext.verify { it.shouldBe(shardId) }
            checkpoint.flag()
        }.start()

        // Command receiver not ready to receive command broadcasts
        commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()

        commandReceiver.readyForShardConsumeCommands()
        commandSender.trySendShardConsumeCmd(shardId).shouldBeTrue()
    }

    @Test
    override fun command_receiver_fail(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val commandSender = RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
            testContext.failNow(Exception("initiator consume shard command handler should not get called"))
        }.start()

        val commandReceiver = RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
            testContext.verify { it.shouldBe(shardId) }
            checkpoint.flag()
            throw Exception("Test exception on Redis remote command receiver")
        }.start()

        // Command receiver not ready to receive command broadcasts
        commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()

        commandReceiver.readyForShardConsumeCommands()
        commandSender.trySendShardConsumeCmd(shardId).shouldBeTrue()
    }

    @Test
    override fun command_to_ifself(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val commandSenderAndReceiver = RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
            testContext.verify { it.shouldBe(shardId) }
            checkpoint.flag()
        }.start()

        // Command receiver not ready to receive command broadcasts
        commandSenderAndReceiver.trySendShardConsumeCmd(shardId).shouldBeFalse()

        commandSenderAndReceiver.readyForShardConsumeCommands()
        commandSenderAndReceiver.trySendShardConsumeCmd(shardId).shouldBeTrue()
    }

    @Test
    override fun multiple_potential_receivers(testContext: VertxTestContext) =
        testContext.async(2) { checkpoint ->
            val commandSender = RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
                testContext.failNow(Exception("initiator consume shard command handler should not get called"))
            }.start()

            val commandReceivers = listOf(
                RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
                    testContext.verify { it.shouldBe(shardId) }
                    checkpoint.flag()
                }.start(),
                RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
                    testContext.verify { it.shouldBe(shardId) }
                    checkpoint.flag()
                }.start()
            )

            // Command receiver not accepts command broadcasts
            commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()

            commandReceivers.forEach { it.readyForShardConsumeCommands() }
            commandSender.trySendShardConsumeCmd(shardId).shouldBeTrue()
        }


    @Test
    override fun no_command_received_if_not_ready(testContext: VertxTestContext) =
        testContext.asyncDelayed(1, 200) { checkpoint ->
            val commandSender = RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
                testContext.failNow(Exception("initiator consume shard command handler should not get called"))
            }.start()

            val commandReceiver = RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName) {
                testContext.verify { it.shouldBe(shardId) }
                checkpoint.flag()
            }.start()

            commandReceiver.readyForShardConsumeCommands()
            commandSender.trySendShardConsumeCmd(shardId).shouldBeTrue()
            commandReceiver.notReadyForShardConsumeCommands()
            commandSender.trySendShardConsumeCmd(shardId).shouldBeFalse()
        }
}
