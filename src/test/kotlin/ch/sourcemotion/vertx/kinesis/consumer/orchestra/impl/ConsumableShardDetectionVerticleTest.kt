package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.ConsumableShardDetectionService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.vertx.core.eventbus.ReplyException
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit

internal class ConsumableShardDetectionVerticleTest : AbstractKinesisAndRedisTest() {

    @Test
    internal fun consumable_shard(tc: VertxTestContext) = tc.async {
        val shardId =
            kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()

        val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
        consumableShardDetectionService.getConsumableShards().await().shouldContainExactly(shardId)
    }

    @Test
    internal fun consumable_shards(tc: VertxTestContext) = tc.async {
        val shardCount = 10
        val consumableShardIds =
            kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount).shards().map { it.shardIdTyped() }

        val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
        consumableShardDetectionService.getConsumableShards().await().shouldContainExactlyInAnyOrder(consumableShardIds)
    }

    @Test
    internal fun consumable_shards_not_existing_stream(tc: VertxTestContext) = tc.async {
        val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
        shouldThrow<ReplyException> { consumableShardDetectionService.getConsumableShards().await() }
    }

    @Test
    internal fun already_consumed_shard_not_detected_as_consumable(tc: VertxTestContext) = tc.async {
        val shardIdInProgress =
            kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()

        shardStatePersistenceService.flagShardInProgress(shardIdInProgress)

        val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
        consumableShardDetectionService.getConsumableShards().await().shouldBeEmpty()
    }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun one_split_child_already_consumed(tc: VertxTestContext) = tc.async {
        val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
        kinesisClient.splitShardFair(parentShard)
        val streamDescriptionAfterSplit =
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child
        val childShardIds = streamDescriptionAfterSplit.shardIds().filterNot { it == parentShard.shardIdTyped() }
        shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())
        // One split child is already in progress
        shardStatePersistenceService.flagShardInProgress(childShardIds.first())

        val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
        consumableShardDetectionService.getConsumableShards().await().shouldContainExactly(childShardIds.last())
    }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun split_children_detected_if_parent_finished(tc: VertxTestContext) = tc.async {
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            kinesisClient.splitShardFair(parentShard)
            val streamDescriptionAfterSplit = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child
            val childShardIds = streamDescriptionAfterSplit.shardIds().filterNot { it == parentShard.shardIdTyped() }
            shardStatePersistenceService.saveFinishedShard(parentShard.shardIdTyped(), Duration.ofHours(1).toMillis())

            val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
            consumableShardDetectionService.getConsumableShards().await().shouldContainExactlyInAnyOrder(childShardIds)
        }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun split_children_not_detected_if_parent_not_finished(tc: VertxTestContext) = tc.async {
            val parentShard = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first()
            kinesisClient.splitShardFair(parentShard)
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child

            val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
            consumableShardDetectionService.getConsumableShards().await().shouldContainExactly(parentShard.shardIdTyped())
        }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun only_merge_parents_detected_if_not_finished(tc: VertxTestContext) = tc.async {
            val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
            val parentShardIds = parentShards.map { it.shardIdTyped() }
            kinesisClient.mergeShards(parentShards)
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child

            val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
            consumableShardDetectionService.getConsumableShards().await().shouldContainExactlyInAnyOrder(parentShardIds)
        }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun only_not_finished_merge_parent_detected(tc: VertxTestContext) = tc.async {
            val parentShards = kinesisClient.createAndGetStreamDescriptionWhenActive(2).shards()
            val parentShardIds = parentShards.map { it.shardIdTyped() }
            kinesisClient.mergeShards(parentShards)
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) // We have to wait until stream get ACTIVE state, otherwise several detection runs will happen before child
            shardStatePersistenceService.saveFinishedShard(
                parentShardIds.first(),
                Duration.ofHours(1).toMillis()
            )

            val consumableShardDetectionService = deployConsumableShardDetectorVerticle()
            consumableShardDetectionService.getConsumableShards().await().shouldContainExactly(parentShardIds.last())
        }

    private suspend fun deployConsumableShardDetectorVerticle(): ConsumableShardDetectionService {
        deployTestVerticle<ConsumableShardDetectionVerticle>(
            ConsumableShardDetectionVerticle.Options(
                TEST_CLUSTER_ORCHESTRA_NAME
            )
        )
        return ConsumableShardDetectionService.createService(vertx)
    }
}
