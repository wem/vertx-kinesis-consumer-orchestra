package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.EventBusAddr
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication.OrchestraCommunication
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.eventbus.completionHandlerAwait
import io.vertx.kotlin.core.eventbus.requestAwait
import kotlinx.coroutines.launch
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class ReshardingOrganizerVerticleTest : AbstractKinesisAndRedisTest() {

    private val eventBusAddr = EventBusAddr(TEST_CLUSTER_ORCHESTRA_NAME).resharding

    @BeforeEach
    internal fun setUp(testContext: VertxTestContext) = testContext.async {
        val options = ReshardingOrganizerVerticle.Options(TEST_CLUSTER_ORCHESTRA_NAME, redisHeimdallOptions, false)
        deployTestVerticle<ReshardingOrganizerVerticle>(options)
    }

    /**
     * On merge resharding event, both parent shard must get stopped.
     */
    @Test
    internal fun stop_consumer_cmd_on_all_merge_resharding_events(testContext: VertxTestContext) =
        testContext.async(2) { checkpoint ->
            val (childShardId, parentShardIds) = createStreamAndMerge()

            eventBus.consumer<ShardId>(eventBusAddr.stopConsumerCmd) { msg ->
                defaultTestScope.launch {
                    val sequenceNumber =
                        shardStatePersistenceService.getConsumerShardSequenceNumber(childShardId)
                    testContext.verify {
                        sequenceNumber.shouldNotBeNull()
                        parentShardIds.shouldContain(msg.body())
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }.completionHandlerAwait()

            parentShardIds.forEach { parentShardId ->
                eventBus.requestAwait<Unit>(
                    eventBusAddr.reshardingNotification,
                    MergeReshardingEvent(parentShardId, childShardId)
                )
            }
        }

    /**
     * On merge resharding, the child shard should get started to consume after both parent are finished.
     */
    @Test
    internal fun start_consumer_cmd_on_second_merge_parent_event(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val (childShardId, parentShardIds) = createStreamAndMerge()

            eventBus.consumer<ShardId>(eventBusAddr.startConsumerCmd) { msg ->
                defaultTestScope.launch {
                    val sequenceNumber = shardStatePersistenceService.getConsumerShardSequenceNumber(childShardId)
                    testContext.verify {
                        sequenceNumber.shouldNotBeNull()
                        msg.body().shouldBe(childShardId)
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }.completionHandlerAwait()

            eventBus.requestAwait<Unit>(
                eventBusAddr.reshardingNotification,
                MergeReshardingEvent(parentShardIds.first(), childShardId)
            )
            eventBus.requestAwait<Unit>(
                eventBusAddr.reshardingNotification,
                MergeReshardingEvent(parentShardIds.last(), childShardId)
            )
        }

    @Test
    internal fun start_consumer_cmd_from_remote(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val shardId = ShardIdGenerator.generateShardId(0)

            eventBus.consumer<ShardId>(eventBusAddr.startConsumerCmd) { msg ->
                testContext.verify { msg.body().shouldBe(shardId) }
                msg.ack()
                checkpoint.flag()
            }.completionHandlerAwait()

            // Set "this" VCKO instance in the ready for start consumer command state, so it will accept
            // start consumer command from remote and also the "this" instance.
            eventBus.requestAwait<Unit>(eventBusAddr.readyForStartConsumerCmd, null)

            val remoteVkcoInstance = OrchestraCommunication.create(
                vertx,
                redisHeimdallOptions,
                TEST_CLUSTER_ORCHESTRA_NAME,
                false
            ) {}

            remoteVkcoInstance.trySendShardConsumeCmd(shardId)
        }

    @Test
    internal fun split_resharding(testContext: VertxTestContext) =
        testContext.async(3) { checkpoint ->
            val (childShardIds, parentShardId) = createStreamAndSplit()

            eventBus.consumer<ShardId>(eventBusAddr.stopConsumerCmd) { msg ->
                val shardId = msg.body()
                defaultTestScope.launch {
                    val sequenceNumber = shardStatePersistenceService.getConsumerShardSequenceNumber(shardId)
                    testContext.verify {
                        shardId.shouldBe(parentShardId)
                        sequenceNumber.shouldNotBeNull()
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }.completionHandlerAwait()

            eventBus.consumer<ShardId>(eventBusAddr.startConsumerCmd) { msg ->
                val shardId = msg.body()
                defaultTestScope.launch {
                    val sequenceNumber = shardStatePersistenceService.getConsumerShardSequenceNumber(shardId)
                    testContext.verify {
                        sequenceNumber.shouldNotBeNull()
                        shardId.shouldBe(childShardIds.first())
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }.completionHandlerAwait()

            val remoteVkcoInstance = OrchestraCommunication.create(
                vertx,
                redisHeimdallOptions,
                TEST_CLUSTER_ORCHESTRA_NAME,
                false
            ) {
                testContext.verify { it.shouldBe(childShardIds.last()) }
                checkpoint.flag()
            }
            remoteVkcoInstance.readyForShardConsumeCommands()

            eventBus.requestAwait<Unit>(
                eventBusAddr.reshardingNotification,
                SplitReshardingEvent(parentShardId, childShardIds)
            )
        }

    @Test
    internal fun split_resharding_remote_start_consumer_called_if_local_did_fail(testContext: VertxTestContext) =
        testContext.async(3) { checkpoint ->
            val (childShardIds, parentShardId) = createStreamAndSplit()

            eventBus.consumer<ShardId>(eventBusAddr.stopConsumerCmd) { msg ->
                testContext.verify { msg.body().shouldBe(parentShardId) }
                defaultTestScope.launch {
                    childShardIds.forEach { childShardId ->
                        val sequenceNumber = shardStatePersistenceService.getConsumerShardSequenceNumber(childShardId)
                        testContext.verify {
                            sequenceNumber.shouldNotBeNull()
                        }
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }.completionHandlerAwait()

            eventBus.consumer<ShardId>(eventBusAddr.startConsumerCmd) { msg ->
                testContext.verify { msg.body().shouldBe(childShardIds.first()) }
                msg.fail(0, "Test start failure")
                checkpoint.flag()
            }.completionHandlerAwait()

            val remoteVkcoInstance = OrchestraCommunication.create(
                vertx,
                redisHeimdallOptions,
                TEST_CLUSTER_ORCHESTRA_NAME,
                false
            ) {
                testContext.verify { it.shouldBe(childShardIds.last()) }
                checkpoint.flag()
            }
            remoteVkcoInstance.readyForShardConsumeCommands()

            eventBus.requestAwait<Unit>(
                eventBusAddr.reshardingNotification,
                SplitReshardingEvent(parentShardId, childShardIds)
            )
        }

    @Test
    internal fun split_resharding_local_start_consumer_called_if_no_remote_available(testContext: VertxTestContext) =
        testContext.async(2) { checkpoint ->
            val (childShardIds, parentShardId) = createStreamAndSplit()

            eventBus.consumer<ShardId>(eventBusAddr.stopConsumerCmd) { msg ->
                testContext.verify { msg.body().shouldBe(parentShardId) }
                defaultTestScope.launch {
                    childShardIds.forEach { childShardId ->
                        val sequenceNumber = shardStatePersistenceService.getConsumerShardSequenceNumber(childShardId)
                        testContext.verify {
                            sequenceNumber.shouldNotBeNull()
                        }
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }.completionHandlerAwait()

            eventBus.consumer<ShardId>(eventBusAddr.startConsumerCmd) { msg ->
                testContext.verify { msg.body().shouldBe(childShardIds.first()) }
                msg.ack()
                checkpoint.flag()
            }.completionHandlerAwait()

            eventBus.requestAwait<Unit>(
                eventBusAddr.reshardingNotification,
                SplitReshardingEvent(parentShardId, childShardIds)
            )
        }

    private suspend fun createStreamAndSplit(): Pair<List<ShardId>, ShardId> {
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        val parentShard = streamDescription.shards().first()
        kinesisClient.splitShardFair(parentShard)
        val childShards = kinesisClient.streamDescriptionWhenActiveAwait(streamDescription.streamName()).shards()
        return childShards.map { it.shardIdTyped() } to parentShard.shardIdTyped()
    }

    private suspend fun createStreamAndMerge(): Pair<ShardId, List<ShardId>> {
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(2)
        val parentShards = streamDescription.shards()
        val parentShardIds = parentShards.map { it.shardIdTyped() }
        kinesisClient.mergeShards(parentShards)
        val childShardId = kinesisClient.streamDescriptionWhenActiveAwait(streamDescription.streamName()).shards()
            .first { parentShardIds.contains(it.shardIdTyped()).not() }.shardIdTyped()
        return childShardId to parentShardIds
    }
}
