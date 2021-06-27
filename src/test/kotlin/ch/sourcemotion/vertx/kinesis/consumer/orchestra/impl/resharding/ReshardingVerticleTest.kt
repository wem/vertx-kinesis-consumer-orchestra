package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.EventBusAddr
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StopConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.launch
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class ReshardingVerticleTest : AbstractKinesisAndRedisTest() {

    @BeforeEach
    internal fun setUp() = asyncBeforeOrAfter {
        val options = ReshardingVerticle.Options(TEST_CLUSTER_ORCHESTRA_NAME, redisHeimdallOptions)
        deployTestVerticle<ReshardingVerticle>(options)
    }

    /**
     * On merge resharding event, both parent shard must get stopped.
     */
    @Test
    internal fun stop_consumer_cmd_on_all_merge_resharding_events(testContext: VertxTestContext) =
        testContext.async(2) { checkpoint ->
            val (childShardId, parentShardIds) = createStreamAndMerge()

            eventBus.consumer<StopConsumerCmd>(EventBusAddr.consumerControl.stopConsumerCmd) { msg ->
                defaultTestScope.launch {
                    val sequenceNumber =
                        shardStatePersistenceService.getConsumerShardSequenceNumber(childShardId)
                    testContext.verify {
                        sequenceNumber.shouldNotBeNull()
                        parentShardIds.shouldContain(msg.body().shardId)
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }.completion().await()

            parentShardIds.forEach { parentShardId ->
                eventBus.request<Unit>(
                    EventBusAddr.resharding.notification,
                    MergeReshardingEvent(parentShardId, childShardId)
                ).await()
            }
        }

    /**
     * On split resharding the parent should be stopped and the first child consume command should send immediately.
     */
    @Test
    internal fun split_resharding(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val (childShardIds, parentShardId) = createStreamAndSplit()

            eventBus.consumer<StopConsumerCmd>(EventBusAddr.consumerControl.stopConsumerCmd) { msg ->
                val shardId = msg.body().shardId
                defaultTestScope.launch {
                    val sequenceNumber = shardStatePersistenceService.getConsumerShardSequenceNumber(shardId)
                    testContext.verify {
                        shardId.shouldBe(parentShardId)
                        sequenceNumber.shouldBeNull()
                    }
                    msg.ack()
                    checkpoint.flag()
                }
            }.completionHandlerAwait()

            eventBus.request<Unit>(
                EventBusAddr.resharding.notification,
                SplitReshardingEvent(parentShardId, childShardIds)
            ).await()
        }

    private suspend fun createStreamAndSplit(): Pair<List<ShardId>, ShardId> {
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        val parentShard = streamDescription.shards().first()
        kinesisClient.splitShardFair(parentShard)
        val childShards = kinesisClient.streamDescriptionWhenActiveAwait(streamDescription.streamName()).shards()
            .filterNot { it.shardId() == streamDescription.shards().first().shardId() }
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
