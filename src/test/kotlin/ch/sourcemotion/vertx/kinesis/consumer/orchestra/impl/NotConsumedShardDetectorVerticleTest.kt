package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumersCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.eventbus.requestAwait
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test
import java.time.Duration

internal class NotConsumedShardDetectorVerticleTest : AbstractKinesisAndRedisTest() {

    private companion object {
        val defaultOptions = NotConsumedShardDetectorVerticle.Options(
            TEST_CLUSTER_ORCHESTRA_NAME,
            1,
            100,
            ShardIteratorStrategy.EXISTING_OR_LATEST
        )
    }

    @Test
    internal fun not_consumed_shard(testContext: VertxTestContext) = testContext.async(1) {  checkpoint ->
        val notConsumedShardId =
            kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactly(notConsumedShardId)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployNotConsumedShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun not_consumed_shards(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val shardCount = 10
        val notConsumedShardIds =
            kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount).shards().map { it.shardIdTyped() }

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactly(notConsumedShardIds)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployNotConsumedShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = shardCount))
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun more_not_consumed_shards_then_can_start(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val shardCount = 10
        val notConsumedShardIds =
            kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount).shards().map { it.shardIdTyped() }

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldHaveSize(1)
                cmd.shardIds.first().shouldBe(notConsumedShardIds.first())
            }
            msg.ack()
            checkpoint.flag()
        }

        deployNotConsumedShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun already_started_consumers_and_not_consumed_detected(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val notConsumedShardId =
            kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactly(notConsumedShardId)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployNotConsumedShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 2))
        sendShardsConsumedCountNotification(1)
    }

    @Test
    internal fun shard_consuming_stopped(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val shardId = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()
        shardStatePersistenceService.flagShardInProgress(shardId)

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactly(shardId)
                cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployNotConsumedShardDetectorVerticle(defaultOptions.copy(initialIteratorStrategy = ShardIteratorStrategy.FORCE_LATEST))
        sendShardsConsumedCountNotification(0)
        // We wait some detection rounds
        delay(defaultOptions.detectionInterval * 2)

        shardStatePersistenceService.flagShardNoMoreInProgress(shardId)
    }

    @Test
    internal fun split_child_not_consumed(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
        kinesisClient.splitShardFair(parentShard)
        val childShardIds = listOf(ShardIdGenerator.generateShardId(1), ShardIdGenerator.generateShardId(2))
        shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())
        // One split child is already in progress
        shardStatePersistenceService.flagShardInProgress(childShardIds.first())

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldHaveSize(1)
                cmd.shardIds.shouldContainExactly(childShardIds.last())
            }
            msg.ack()
            checkpoint.flag()
        }

        deployNotConsumedShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun split_children_not_consumed(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
        kinesisClient.splitShardFair(parentShard)
        val childShardIds = listOf(ShardIdGenerator.generateShardId(1), ShardIdGenerator.generateShardId(2))
        shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactlyInAnyOrder(childShardIds)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployNotConsumedShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 2))
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun consume_split_parent_and_later_children(testContext: VertxTestContext) = testContext.async(3) { checkpoint ->
        val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
        val childShardIds = listOf(ShardIdGenerator.generateShardId(1), ShardIdGenerator.generateShardId(2))

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                when(cmd.shardIds.size) {
                    1 -> cmd.shardIds.shouldContainExactly(parentShard.shardIdTyped())
                    2 -> cmd.shardIds.shouldContainExactlyInAnyOrder(childShardIds)
                    else -> testContext.failNow(Exception("Start command for unexpected shards ${cmd.shardIds.joinToString()}"))
                }
                repeat(cmd.shardIds.size) { checkpoint.flag() }
            }
            msg.ack()
        }

        deployNotConsumedShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 3))
        sendShardsConsumedCountNotification(0)

        delay(defaultOptions.detectionInterval * 2)

        shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())
        kinesisClient.splitShardFair(parentShard)
    }

    @Test
    internal fun split_children_not_started_if_parent_not_finished(testContext: VertxTestContext) = testContext.asyncDelayed(0, 500) {
        val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
        kinesisClient.splitShardFair(parentShard)

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) {
            testContext.failNow(Exception("Consumer must not be started in the case a parent is not finished"))
        }

        deployNotConsumedShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun merge_child_not_started_if_parents_not_finished(testContext: VertxTestContext) = testContext.asyncDelayed(0, 500) {
        val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
        kinesisClient.mergeShards(parentShards)

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) {
            testContext.failNow(Exception("Consumer must not be started in the case at least one parent is not finished"))
        }

        deployNotConsumedShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun merge_child_not_started_if_parent_not_finished(testContext: VertxTestContext) = testContext.asyncDelayed(0, 500) {
        val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
        kinesisClient.mergeShards(parentShards)
        shardStatePersistenceService.saveFinishedShard(parentShards.first().shardIdTyped(), Duration.ofHours(1).toMillis())

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) {
            testContext.failNow(Exception("Consumer must not be started in the case at least one parent is not finished"))
        }

        deployNotConsumedShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun consume_merge_parents_and_later_child(testContext: VertxTestContext) = testContext.async(3) { checkpoint ->
        val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
        val childShardId = ShardIdGenerator.generateShardId(2)

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                when(cmd.shardIds.size) {
                    1 -> cmd.shardIds.shouldContainExactly(childShardId)
                    2 -> cmd.shardIds.shouldContainExactlyInAnyOrder(parentShards.map { it.shardIdTyped() })
                    else -> testContext.failNow(Exception("Start command for unexpected shards ${cmd.shardIds.joinToString()}"))
                }
                repeat(cmd.shardIds.size) { checkpoint.flag() }
            }
            msg.ack()
        }

        deployNotConsumedShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 3))
        sendShardsConsumedCountNotification(0)

        delay(defaultOptions.detectionInterval * 2)

        parentShards.forEach { shardStatePersistenceService.saveFinishedShard(it.shardIdTyped(), Duration.ofHours(1).toMillis()) }
        kinesisClient.mergeShards(parentShards)
    }

    private suspend fun sendShardsConsumedCountNotification(consumedShards: Int) {
        eventBus.requestAwait<Unit>(EventBusAddr.detection.activeConsumerCountNotification, consumedShards)
    }

    private suspend fun deployNotConsumedShardDetectorVerticle(
        options: NotConsumedShardDetectorVerticle.Options = defaultOptions
    ) {
        deployTestVerticle<NotConsumedShardDetectorVerticle>(options)
    }
}
