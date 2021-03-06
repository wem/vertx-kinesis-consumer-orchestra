package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumersCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.date.shouldBeBetween
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

internal class ConsumableShardDetectionVerticleTest : AbstractKinesisAndRedisTest() {

    private companion object {
        val defaultOptions = ConsumableShardDetectionVerticle.Options(
            TEST_CLUSTER_ORCHESTRA_NAME,
            1,
            100,
            ShardIteratorStrategy.FORCE_LATEST
        )
    }

    @Test
    internal fun consumable_shard(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
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

        deployConsumableShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun consumable_shards(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val shardCount = 10
        val consumableShardIds =
            kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount).shards().map { it.shardIdTyped() }

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactly(consumableShardIds)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = shardCount))
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun more_consumable_shards_then_can_start(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
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

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun already_consumed_shard_not_detected_as_consumable(testContext: VertxTestContext) =
        testContext.asyncDelayed(1, defaultOptions.detectionInterval * 3) { checkpoint ->

            val consumableShardId =
                kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()

            eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    cmd.shardIds.shouldContainExactly(consumableShardId)
                }
                launch {
                    shardStatePersistenceService.flagShardInProgress(consumableShardId)
                    msg.ack()
                    checkpoint.flag()
                }
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun start_consumers_sequential(testContext: VertxTestContext) = testContext.async(5) { checkpoint ->
        val consumableShardIds =
            kinesisClient.createAndGetStreamDescriptionWhenActive(5).shards().map { it.shardIdTyped() }.toMutableList()

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                val startedShardId = cmd.shardIds.shouldHaveSize(1).first()
                consumableShardIds.shouldContain(startedShardId)
                consumableShardIds.remove(startedShardId)
                defaultTestScope.launch {
                    shardStatePersistenceService.flagShardInProgress(startedShardId) // We flag the shard as in progress (like consumer started)
                    msg.ack()
                    checkpoint.flag()
                }
            }
        }

        deployConsumableShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun detect_later_not_proceeded(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val shardId = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()
        shardStatePersistenceService.flagShardInProgress(shardId)

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactly(shardId)
                cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST) // Because command results not from the initial detection run
            }
            msg.ack()
            checkpoint.flag()
        }

        deployConsumableShardDetectorVerticle(defaultOptions.copy(initialIteratorStrategy = ShardIteratorStrategy.FORCE_LATEST))
        sendShardsConsumedCountNotification(0)
        // We wait some detection rounds
        delay(defaultOptions.detectionInterval * 2)

        shardStatePersistenceService.flagShardNoMoreInProgress(shardId)
    }

    @Test
    internal fun one_split_child_already_consumed(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
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
                cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployConsumableShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun split_children_detected_if_parent_finished(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
        kinesisClient.splitShardFair(parentShard)
        val childShardIds = listOf(ShardIdGenerator.generateShardId(1), ShardIdGenerator.generateShardId(2))
        shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactlyInAnyOrder(childShardIds)
                cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 2))
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun detect_split_parent_and_later_children(testContext: VertxTestContext) =
        testContext.async(3) { checkpoint ->
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            val childShardIds = listOf(ShardIdGenerator.generateShardId(1), ShardIdGenerator.generateShardId(2))

            eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    when (cmd.shardIds.size) {
                        1 -> {
                            cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                            cmd.shardIds.shouldContainExactly(parentShard.shardIdTyped())
                        }
                        2 -> {
                            cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
                            cmd.shardIds.shouldContainExactlyInAnyOrder(childShardIds)
                        }
                        else -> testContext.failNow(Exception("Start command for unexpected shards ${cmd.shardIds.joinToString()}"))
                    }
                    repeat(cmd.shardIds.size) { checkpoint.flag() }
                }
                msg.ack()
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 3))
            sendShardsConsumedCountNotification(0)

            delay(defaultOptions.detectionInterval * 2)

            shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())
            kinesisClient.splitShardFair(parentShard)
        }

    @Test
    internal fun split_children_not_detected_if_parent_not_finished(testContext: VertxTestContext) =
        testContext.asyncDelayed(1) { checkpoint ->
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            kinesisClient.splitShardFair(parentShard)

            eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
                val cmd = msg.body()
                testContext.verify {
                    cmd.shardIds.shouldContainExactly(parentShard.shardIdTyped())
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                }
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun only_merge_parents_detected_if_not_finished(testContext: VertxTestContext) =
        testContext.asyncDelayed(1) { checkpoint ->
            val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
            kinesisClient.mergeShards(parentShards)

            eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
                val cmd = msg.body()
                testContext.verify {
                    cmd.shardIds.shouldContainExactlyInAnyOrder(parentShards.map { it.shardIdTyped() })
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                }
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 2))
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun only_not_finished_merge_parent_detected(testContext: VertxTestContext) =
        testContext.asyncDelayed(1) { checkpoint ->
            val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
            kinesisClient.mergeShards(parentShards)
            shardStatePersistenceService.saveFinishedShard(
                parentShards.first().shardIdTyped(),
                Duration.ofHours(1).toMillis()
            )

            eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
                val cmd = msg.body()
                testContext.verify {
                    cmd.shardIds.shouldContainExactly(parentShards.last().shardIdTyped())
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                }
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun detect_merge_parents_and_later_child(testContext: VertxTestContext) =
        testContext.async(3) { checkpoint ->
            val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
            val childShardId = ShardIdGenerator.generateShardId(2)

            var parentsDetected = false
            eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    when (cmd.shardIds.size) {
                        1 -> {
                            parentsDetected.shouldBeTrue()
                            cmd.shardIds.shouldContainExactly(childShardId)
                            cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
                        }
                        2 -> {
                            parentsDetected.shouldBeFalse()
                            parentsDetected = true
                            cmd.shardIds.shouldContainExactlyInAnyOrder(parentShards.map { it.shardIdTyped() })
                            cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                        }
                        else -> testContext.failNow(Exception("Start command for unexpected shards ${cmd.shardIds.joinToString()}"))
                    }
                    repeat(cmd.shardIds.size) { checkpoint.flag() }
                }
                msg.ack()
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 3))
            sendShardsConsumedCountNotification(0)

            delay(defaultOptions.detectionInterval * 2)

            parentShards.forEach {
                shardStatePersistenceService.saveFinishedShard(
                    it.shardIdTyped(),
                    Duration.ofHours(1).toMillis()
                )
            }
            kinesisClient.mergeShards(parentShards)
        }

    @Test
    internal fun detection_not_stopped_if_consumer_not_started(testContext: VertxTestContext) =
        testContext.async(10) { checkpoint ->
            val shard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()

            var startCommandCount = 0
            eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    testContext.verify {
                        cmd.shardIds.shouldContainExactly(shard.shardIdTyped())
                        if (startCommandCount++ == 0) {
                            cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                        } else {
                            cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
                        }
                    }
                    checkpoint.flag()
                }
                msg.ack()
                defaultTestScope.launch { sendShardsConsumedCountNotification(0) }
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun detection_stop_and_restart_later(testContext: VertxTestContext) = testContext.async(2) { checkpoint ->
        val shard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()

        var startCommandNbr = 0

        val delayToRestartDetection = defaultOptions.detectionInterval * 2
        var detectionStopNotificationTimestamp: Instant? = null

        eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardIds.shouldContainExactly(shard.shardIdTyped())
                if (++startCommandNbr == 1) {
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                    defaultTestScope.launch {
                        detectionStopNotificationTimestamp = Instant.now()
                        // We send the notification with consumer max as count of current consumers. This will stop the detection.
                        sendShardsConsumedCountNotification(1)
                        msg.ack()

                        delay(delayToRestartDetection)
                        // We send the notification with 0 count of current consumers. This will restart the detection
                        sendShardsConsumedCountNotification(0)

                        checkpoint.flag()
                    }
                } else {
                    testContext.verify {
                        startCommandNbr.shouldBe(2)
                        cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)

                        val expectedEarliestDetectionRestartTimestamp =
                            detectionStopNotificationTimestamp.shouldNotBeNull().plusMillis(delayToRestartDetection)

                        val expectedLatestDetectionRestartTimestamp =
                            detectionStopNotificationTimestamp.shouldNotBeNull().plusMillis(delayToRestartDetection * 2)

                        val detectionRestartTimestamp = Instant.now()
                        detectionRestartTimestamp.shouldBeBetween(
                            expectedEarliestDetectionRestartTimestamp,
                            expectedLatestDetectionRestartTimestamp
                        )
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }
        }

        deployConsumableShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun detection_not_stopped_on_consumer_start_failure(testContext: VertxTestContext) =
        testContext.async(10) { checkpoint ->
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            kinesisClient.splitShardFair(parentShard)

            var startCommandNbr = 0
            eventBus.consumer<StartConsumersCmd>(EventBusAddr.consumerControl.startConsumersCmd) { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    cmd.shardIds.shouldContainExactly(parentShard.shardIdTyped())
                    if (++startCommandNbr == 1) {
                        cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                    } else {
                        cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
                    }
                }

                defaultTestScope.launch {
                    msg.fail(0, "Test failure")
                    checkpoint.flag()
                }
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    private suspend fun sendShardsConsumedCountNotification(consumedShards: Int) {
        eventBus.request<Unit>(EventBusAddr.detection.consumedShardCountNotification, consumedShards).await()
    }

    private suspend fun deployConsumableShardDetectorVerticle(
        options: ConsumableShardDetectionVerticle.Options = defaultOptions
    ) {
        deployTestVerticle<ConsumableShardDetectionVerticle>(options)
    }
}
