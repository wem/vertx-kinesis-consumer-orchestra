package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumerCmd.FailureCodes.CONSUMER_START_FAILURE
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.date.shouldBeBetween
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.core.eventbus.Message
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

internal class ConsumableShardDetectionVerticleTest : AbstractKinesisAndRedisTest() {

    private companion object : KLogging()

    private val defaultOptions = ConsumableShardDetectionVerticle.Options(
        TEST_CLUSTER_ORCHESTRA_NAME,
        1,
        100,
        10000,
        100,
        initialIteratorStrategy = ShardIteratorStrategy.FORCE_LATEST,
        redisHeimdallOptions = redisHeimdallOptions
    )

    @Test
    internal fun consumable_shard(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val shardId =
            kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()

        startCmdConsumer { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardId.shouldBe(shardId)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployConsumableShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun consumable_shards(testContext: VertxTestContext) {
        val shardCount = 10
        testContext.async(shardCount) { checkpoint ->
            val consumableShardIds =
                kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount).shards().map { it.shardIdTyped() }

            val receivedShardIds = ArrayList<ShardId>()
            startCmdConsumer { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    val shardId = cmd.shardId
                    consumableShardIds.shouldContain(shardId)
                    receivedShardIds.shouldNotContain(shardId)
                    receivedShardIds.add(shardId)
                }
                msg.ack()
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = shardCount))
            sendShardsConsumedCountNotification(0)
        }
    }

    @Test
    internal fun more_consumable_shards_then_can_start(testContext: VertxTestContext) =
        testContext.asyncDelayed(1, defaultOptions.detectionInterval * 5) { checkpoint ->
            val shardCount = 10
            val shards =
                kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount).shards().map { it.shardIdTyped() }

            startCmdConsumer { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    cmd.shardId.shouldBe(shards.first())
                }
                defaultTestScope.launch {
                    sendShardsConsumedCountNotification(1)
                    msg.ack()
                }
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
            startCmdConsumer { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    cmd.shardId.shouldBe(consumableShardId)
                }
                defaultTestScope.launch {
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

        startCmdConsumer { msg ->
            testContext.verify {
                val cmd = msg.body()
                val shardId = cmd.shardId
                consumableShardIds.shouldContain(shardId)
                consumableShardIds.remove(shardId)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployConsumableShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Test
    internal fun shard_no_more_in_progress(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val shardId = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()
        shardStatePersistenceService.flagShardInProgress(shardId)

        startCmdConsumer { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardId.shouldBe(shardId)
                cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST) // Because the first detection runs shouldn't detect consumable shard as flagged in progress
            }
            msg.ack()
            checkpoint.flag()
        }

        deployConsumableShardDetectorVerticle(defaultOptions.copy(initialIteratorStrategy = ShardIteratorStrategy.FORCE_LATEST))
        sendShardsConsumedCountNotification(0)
        // We wait some detection rounds
        delay(defaultOptions.detectionInterval * 10) // The requests against Kinesis could take longer, otherwise the flag get removed too early

        shardStatePersistenceService.flagShardNoMoreInProgress(shardId)
    }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun one_split_child_already_consumed(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
        kinesisClient.splitShardFair(parentShard)
        kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child
        val childShardIds = listOf(ShardIdGenerator.generateShardId(1), ShardIdGenerator.generateShardId(2))
        shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())
        // One split child is already in progress
        shardStatePersistenceService.flagShardInProgress(childShardIds.first())

        startCmdConsumer { msg ->
            testContext.verify {
                val cmd = msg.body()
                cmd.shardId.shouldBe(childShardIds.last())
                cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
            }
            msg.ack()
            checkpoint.flag()
        }

        deployConsumableShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun split_children_detected_if_parent_finished(testContext: VertxTestContext) =
        testContext.async(2) { checkpoint ->
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            kinesisClient.splitShardFair(parentShard)
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child
            val childShardIds = listOf(ShardIdGenerator.generateShardId(1), ShardIdGenerator.generateShardId(2))
            shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())

            val startedShardIds = ArrayList<ShardId>()
            startCmdConsumer { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    childShardIds.shouldContain(cmd.shardId)
                    startedShardIds.shouldNotContain(cmd.shardId)
                    startedShardIds.add(cmd.shardId)
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                }
                msg.ack()
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 2))
            sendShardsConsumedCountNotification(0)
        }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun detect_split_parent_and_later_children(testContext: VertxTestContext) =
        testContext.async(3) { checkpoint ->
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            val childShardIds = listOf(ShardIdGenerator.generateShardId(1), ShardIdGenerator.generateShardId(2))

            val startedChildShardIds = ArrayList<ShardId>()
            var parentStarted = false
            startCmdConsumer { msg ->
                testContext.verify {
                    val cmd = msg.body()
                    val startedShardId = cmd.shardId
                    if (parentStarted.not()) {
                        parentStarted = true
                        startedShardId.shouldBe(parentShard.shardIdTyped())
                        defaultTestScope.launch {
                            shardStatePersistenceService.saveFinishedShard(
                                parentShard.shardIdTyped(),
                                Duration.ofHours(1).toMillis()
                            )
                            kinesisClient.splitShardFair(parentShard)
                            checkpoint.flag()
                        }
                        msg.ack()
                    } else {
                        parentStarted.shouldBeTrue()
                        cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
                        startedChildShardIds.shouldNotContain(startedShardId)
                        startedChildShardIds.add(startedShardId)
                        childShardIds.shouldContain(startedShardId)
                        msg.ack()
                        checkpoint.flag()
                    }
                }
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 2))
            sendShardsConsumedCountNotification(0)
        }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun split_children_not_detected_if_parent_not_finished(testContext: VertxTestContext) =
        testContext.asyncDelayed(1) { checkpoint ->
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            kinesisClient.splitShardFair(parentShard)
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child

            startCmdConsumer { msg ->
                val cmd = msg.body()
                testContext.verify {
                    cmd.shardId.shouldBe(parentShard.shardIdTyped())
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                }
                msg.ack()
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun only_merge_parents_detected_if_not_finished(testContext: VertxTestContext) =
        testContext.asyncDelayed(2) { checkpoint ->
            val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
            val parentShardIds = parentShards.map { it.shardIdTyped() }
            kinesisClient.mergeShards(parentShards)
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child

            val startedShardIds = ArrayList<ShardId>()
            startCmdConsumer { msg ->
                val cmd = msg.body()
                val startedShardId = cmd.shardId
                testContext.verify {
                    parentShardIds.shouldContain(startedShardId)
                    startedShardIds.shouldNotContain(startedShardId)
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                }
                checkpoint.flag()
                msg.ack()
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 2))
            sendShardsConsumedCountNotification(0)
        }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun only_not_finished_merge_parent_detected(testContext: VertxTestContext) =
        testContext.asyncDelayed(1) { checkpoint ->
            val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
            kinesisClient.mergeShards(parentShards)
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child
            shardStatePersistenceService.saveFinishedShard(
                parentShards.first().shardIdTyped(),
                Duration.ofHours(1).toMillis()
            )

            startCmdConsumer { msg ->
                val cmd = msg.body()
                testContext.verify {
                    cmd.shardId.shouldBe(parentShards.last().shardIdTyped())
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                }
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun detect_merge_parents_and_later_child(testContext: VertxTestContext) =
        testContext.async(3) { checkpoint ->
            val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
            val parentShardIds = parentShards.map { it.shardIdTyped() }
            val childShardId = ShardIdGenerator.generateShardId(2)

            val finishAndMergeParentShards = suspend {
                kinesisClient.mergeShards(parentShards)
                parentShards.forEach {
                    shardStatePersistenceService.saveFinishedShard(it.shardIdTyped(), Duration.ofHours(1).toMillis())
                }
            }

            val startedShardIds = ArrayList<ShardId>()
            startCmdConsumer { msg ->
                val cmd = msg.body()
                val shardId = cmd.shardId
                testContext.verify {
                    if (startedShardIds.size < 2) {
                        startedShardIds.shouldNotContain(shardId)
                        startedShardIds.add(shardId)
                        parentShardIds.shouldContain(shardId)
                        cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                        if (startedShardIds.size == 2) {
                            defaultTestScope.launch {
                                finishAndMergeParentShards()
                                msg.ack()
                            }
                        } else {
                            msg.ack()
                        }
                    } else {
                        cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
                        shardId.shouldBe(childShardId)
                    }
                    checkpoint.flag()
                }
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 3))
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun detection_not_stopped_if_consumer_not_started(testContext: VertxTestContext) =
        testContext.async(10) { checkpoint ->
            val shard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()

            var startCommandCount = 0
            startCmdConsumer { msg ->
                val cmd = msg.body()
                testContext.verify {
                    testContext.verify {
                        cmd.shardId.shouldBe(shard.shardIdTyped())
                        if (startCommandCount++ == 0) {
                            cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                        } else {
                            cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
                        }
                    }
                    checkpoint.flag()
                }
                defaultTestScope.launch {
                    shardStatePersistenceService.flagShardNoMoreInProgress(cmd.shardId) // We simulate a very slow consumer here
                    msg.ack()
                }
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun detection_stop_and_restart_later(testContext: VertxTestContext) = testContext.async(2) { checkpoint ->
        val shard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()

        var firstCmdReceived = false

        val delayToRestartDetection = defaultOptions.detectionInterval * 5
        var detectionStopNotificationTimestamp: Instant? = null

        startCmdConsumer { msg ->
            val cmd = msg.body()
            testContext.verify { cmd.shardId.shouldBe(shard.shardIdTyped()) }
            if (firstCmdReceived.not()) {
                firstCmdReceived = true
                testContext.verify { cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST) }
                defaultTestScope.launch {
                    // We send the notification with consumer max as count of current consumers. This will stop the detection.
                    sendShardsConsumedCountNotification(1)
                    detectionStopNotificationTimestamp = Instant.now()
                    msg.ack()

                    delay(delayToRestartDetection)
                    // We send the notification with 0 count of current consumers. This will restart the detection
                    shardStatePersistenceService.flagShardNoMoreInProgress(shard.shardIdTyped())
                    sendShardsConsumedCountNotification(0)
                    checkpoint.flag()
                }
            } else {
                testContext.verify {
                    cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)

                    val expectedEarliestDetectionRestartTimestamp =
                        detectionStopNotificationTimestamp.shouldNotBeNull().plusMillis(delayToRestartDetection)

                    val expectedLatestDetectionRestartTimestamp =
                        detectionStopNotificationTimestamp.shouldNotBeNull()
                            .plusMillis(delayToRestartDetection * 2 + KINESIS_API_LATENCY_MILLIS)

                    val detectionRestartTimestamp = Instant.now()
                    detectionRestartTimestamp.shouldBeBetween(
                        expectedEarliestDetectionRestartTimestamp,
                        expectedLatestDetectionRestartTimestamp
                    )
                }
                checkpoint.flag()
            }
        }

        deployConsumableShardDetectorVerticle()
        sendShardsConsumedCountNotification(0)
    }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun detection_not_stopped_on_consumer_start_failure(testContext: VertxTestContext) =
        testContext.async(10) { checkpoint ->
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            kinesisClient.splitShardFair(parentShard)

            var startCommandNbr = 0
            startCmdConsumer { msg ->
                val cmd = msg.body()
                testContext.verify {
                    cmd.shardId.shouldBe(parentShard.shardIdTyped())
                    if (++startCommandNbr == 1) {
                        cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
                    } else {
                        cmd.iteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
                    }
                }
                msg.fail(CONSUMER_START_FAILURE, "")
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun in_progress_flag_removed_if_start_failed(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val shard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            val shardId = shard.shardIdTyped()

            startCmdConsumer { msg ->
                val cmd = msg.body()
                testContext.verify {
                    cmd.shardId.shouldBe(shardId)
                }
                defaultTestScope.launch {
                    // To stop detection, so in progress flag will not get set again
                    sendShardsConsumedCountNotification(1)
                    msg.fail(CONSUMER_START_FAILURE, "")
                    delay(1000)
                    shardStatePersistenceService.getShardIdsInProgress(listOf(shardId)).shouldContain(shardId)
                }
                checkpoint.flag()
            }

            deployConsumableShardDetectorVerticle()
            sendShardsConsumedCountNotification(0)
        }

    @Test
    internal fun high_number_concurrent_detection_verticles(testContext: VertxTestContext) {
        val shardCount = 64
        val verticleCount = shardCount / 2
        testContext.async(shardCount) { checkpoint ->
            val shardIds =
                kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount).shards().map { it.shardIdTyped() }

            val startedShardIds = ArrayList<ShardId>()
            startCmdConsumer { msg ->
                val startedShardId = msg.body().shardId
                testContext.verify {
                    shardIds.shouldContain(startedShardId)
                    startedShardIds.shouldNotContain(startedShardId)
                }
                startedShardIds.add(startedShardId)
                checkpoint.flag()
                msg.ack()
            }

            deployConsumableShardDetectorVerticle(defaultOptions.copy(maxShardCountToConsume = 2), verticleCount)
            publishDetectionStartNotification()
        }
    }

    private suspend fun sendShardsConsumedCountNotification(consumedShards: Int) {
        eventBus.request<Unit>(EventBusAddr.detection.consumedShardCountNotification, consumedShards).await()
    }

    private fun publishDetectionStartNotification() {
        eventBus.publish(EventBusAddr.detection.consumedShardCountNotification, 0)
    }

    private suspend fun deployConsumableShardDetectorVerticle(
        options: ConsumableShardDetectionVerticle.Options = defaultOptions,
        instances: Int = 1
    ) {
        deployTestVerticle<ConsumableShardDetectionVerticle>(options, instances)
    }

    private fun startCmdConsumer(consumer: (Message<StartConsumerCmd>) -> Unit) {
        eventBus.consumer<StartConsumerCmd>(EventBusAddr.consumerControl.startConsumerCmd) { msg ->
            consumer(msg)
        }
    }
}
