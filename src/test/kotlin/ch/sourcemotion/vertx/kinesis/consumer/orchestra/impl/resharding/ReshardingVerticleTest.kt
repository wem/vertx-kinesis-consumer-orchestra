package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.EventBusAddr
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.ConsumerControlService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.StopConsumersCmdResult
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

internal class ReshardingVerticleTest : AbstractKinesisAndRedisTest() {

    /**
     * On merge resharding event, both parent shard must get stopped.
     */
    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun stop_consumer_cmd_on_all_merge_resharding_events(testContext: VertxTestContext) =
        testContext.async(2) { checkpoint ->
            val (childShardId, parentShardIds) = createStreamAndMerge()

            ConsumerControlService.exposeService(vertx, StopOnlyConsumerControlService(defaultTestScope) { shardId ->
                val sequenceNumber =
                    shardStatePersistenceService.getConsumerShardSequenceNumber(childShardId)
                testContext.verify {
                    sequenceNumber.shouldNotBeNull()
                    parentShardIds.shouldContain(shardId)
                }
                checkpoint.flag()
            }).await()

            val options = ReshardingVerticle.Options(TEST_CLUSTER_ORCHESTRA_NAME, redisHeimdallOptions)
            deployTestVerticle<ReshardingVerticle>(options)

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
    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun split_resharding(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val (childShardIds, parentShardId) = createStreamAndSplit()

            ConsumerControlService.exposeService(vertx, StopOnlyConsumerControlService(defaultTestScope) { shardId ->
                val sequenceNumber = shardStatePersistenceService.getConsumerShardSequenceNumber(shardId)
                testContext.verify {
                    shardId.shouldBe(parentShardId)
                    sequenceNumber.shouldBeNull()
                }
                checkpoint.flag()
            }).await()

            val options = ReshardingVerticle.Options(TEST_CLUSTER_ORCHESTRA_NAME, redisHeimdallOptions)
            deployTestVerticle<ReshardingVerticle>(options)

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

private class StopOnlyConsumerControlService(private val scope: CoroutineScope, private val block: suspend (ShardId) -> Unit) : ConsumerControlService {
    override fun stopConsumer(shardId: ShardId): Future<Void> {
        val p = Promise.promise<Void>()
        scope.launch {
            block(shardId)
            p.complete()
        }
        return p.future()
    }

    override fun stopConsumers(consumerCount: Int): Future<StopConsumersCmdResult> {
        TODO("Not yet implemented")
    }

    override fun startConsumers(shardIds: List<ShardId>): Future<Int> {
        TODO("Not yet implemented")
    }
}
