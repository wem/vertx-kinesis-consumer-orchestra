package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.StopConsumersCmdResult
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import org.junit.jupiter.api.Test
import java.util.*

internal class RedisBalancingNodeCommunicationTest : AbstractRedisTest() {


    @Test
    internal fun send_start_consumer_cmd(testContext: VertxTestContext) = testContext.async {
        val nodeOneId = orchestraClusterNodeId()
        val nodeTwoId = orchestraClusterNodeId()

        val expectedShardIdsToStart = listOf(ShardId("1"), ShardId("2"))

        val nodeOneComm = RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeOneId, 10000, {
            Future.failedFuture("Not under test")
        }) {
            Future.failedFuture("Not under test")
        }.start()

        RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeTwoId, 10000, {
            Future.failedFuture("Not under test")
        }) { shardIdsToStart ->
            testContext.verify { shardIdsToStart.shouldContainExactly(expectedShardIdsToStart) }
            Future.succeededFuture(expectedShardIdsToStart.size)
        }.start()

        nodeOneComm.sendStartConsumersCmd(nodeTwoId, expectedShardIdsToStart).await().shouldBe(expectedShardIdsToStart.size)
    }

    @Test
    internal fun start_consumer_cmd_failed(testContext: VertxTestContext) = testContext.async {
        val nodeOneId = orchestraClusterNodeId()
        val nodeTwoId = orchestraClusterNodeId()

        val expectedShardIdsToStart = listOf(ShardId("1"), ShardId("2"))

        val nodeOneComm = RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeOneId, 10000, {
            Future.failedFuture("Not under test")
        }) {
            Future.failedFuture("Not under test")
        }.start()

        RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeTwoId, 10000, {
            Future.failedFuture("Not under test")
        }) { shardIdsToStart ->
            testContext.verify { shardIdsToStart.shouldContainExactly(expectedShardIdsToStart) }
            Future.failedFuture("Test failure")
        }.start()

        shouldThrow<BalancingException> {
            nodeOneComm.sendStartConsumersCmd(nodeTwoId, expectedShardIdsToStart).await()
        }.message.shouldBe("Test failure")
    }

    @Test
    internal fun start_consumer_cmd_timeout(testContext: VertxTestContext) = testContext.async {
        val nodeOneId = orchestraClusterNodeId()
        val nodeTwoId = orchestraClusterNodeId()

        val expectedShardIdsToStart = listOf(ShardId("1"), ShardId("2"))

        val nodeOneComm = RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeOneId, 1000, {
            Future.failedFuture("Not under test")
        }) {
            Future.failedFuture("Not under test")
        }.start()

        RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeTwoId, 1000, {
            Future.failedFuture("Not under test")
        }) { shardIdsToStart ->
            testContext.verify { shardIdsToStart.shouldContainExactly(expectedShardIdsToStart) }
            val p = Promise.promise<Int>()
            vertx.setTimer(2000) {
                p.complete(expectedShardIdsToStart.size)
            }
            p.future()
        }.start()

        shouldThrow<BalancingException> {
            nodeOneComm.sendStartConsumersCmd(nodeTwoId, expectedShardIdsToStart).await()
        }.message.shouldContain("did takes longer as allowed")
    }

    @Test
    internal fun send_stop_consumer_cmd(testContext: VertxTestContext) = testContext.async {
        val nodeOneId = orchestraClusterNodeId()
        val nodeTwoId = orchestraClusterNodeId()

        val expectedStoppedShardIds = listOf(ShardId("1"), ShardId("2"))
        val expectedShardIdCountToStop = expectedStoppedShardIds.size
        val expectedStopConsumersCmdResult = StopConsumersCmdResult(expectedStoppedShardIds, 0)

        val nodeOneComm = RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeOneId, 10000, {
            Future.failedFuture("Not under test")
        }) {
            Future.failedFuture("Not under test")
        }.start()

        RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeTwoId, 10000, {
            testContext.verify { it.shouldBe(expectedShardIdCountToStop) }
            Future.succeededFuture(StopConsumersCmdResult(expectedStoppedShardIds, 0))
        }) {
            Future.failedFuture("Not under test")
        }.start()

        nodeOneComm.sendStopConsumersCmd(nodeTwoId, expectedShardIdCountToStop).await()
            .shouldBe(expectedStopConsumersCmdResult)
    }

    @Test
    internal fun stop_consumer_cmd_failed(testContext: VertxTestContext) = testContext.async {
        val nodeOneId = orchestraClusterNodeId()
        val nodeTwoId = orchestraClusterNodeId()

        val expectedStoppedShardIds = listOf(ShardId("1"), ShardId("2"))
        val expectedShardIdCountToStop = expectedStoppedShardIds.size

        val nodeOneComm = RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeOneId, 10000, {
            Future.failedFuture("Not under test")
        }) {
            Future.failedFuture("Not under test")
        }.start()

        RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeTwoId, 10000, {
            testContext.verify { it.shouldBe(expectedShardIdCountToStop) }
            Future.failedFuture("Test failure")
        }) {
            Future.failedFuture("Not under test")
        }.start()

        shouldThrow<BalancingException> {
            nodeOneComm.sendStopConsumersCmd(nodeTwoId, expectedShardIdCountToStop).await()
        }.message.shouldBe("Test failure")
    }

    @Test
    internal fun stop_consumer_cmd_timeout(testContext: VertxTestContext) = testContext.async {
        val nodeOneId = orchestraClusterNodeId()
        val nodeTwoId = orchestraClusterNodeId()

        val expectedStoppedShardIds = listOf(ShardId("1"), ShardId("2"))
        val expectedShardIdCountToStop = expectedStoppedShardIds.size

        val nodeOneComm = RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeOneId, 1000, {
            Future.failedFuture("Not under test")
        }) {
            Future.failedFuture("Not under test")
        }.start()

        RedisBalancingNodeCommunication(vertx, redisHeimdallOptions, nodeTwoId, 1000, {
            testContext.verify { it.shouldBe(expectedShardIdCountToStop) }
            val p = Promise.promise<StopConsumersCmdResult>()
            vertx.setTimer(2000) {
                p.complete(StopConsumersCmdResult(expectedStoppedShardIds, 0))
            }
            p.future()
        }) {
            Future.failedFuture("Not under test")
        }.start()

        shouldThrow<BalancingException> {
            nodeOneComm.sendStopConsumersCmd(nodeTwoId, expectedShardIdCountToStop).await()
        }.message.shouldContain("did takes longer as allowed")
    }


    private fun orchestraClusterNodeId() = OrchestraClusterNodeId(
        OrchestraClusterName("OrchestraNodeStateVerticleTest", "some-stream"),
        "${UUID.randomUUID()}"
    )
}