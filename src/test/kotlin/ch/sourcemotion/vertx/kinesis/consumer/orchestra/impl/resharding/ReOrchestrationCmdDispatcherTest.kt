package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test

/**
 * Test when the resharding command dispatching is based on Redis pub sub.
 */
internal class ReOrchestrationCmdDispatcherTest : AbstractKinesisAndRedisTest() {

    @Test
    internal fun redis_based_split_resharding(testContext: VertxTestContext) = asyncTest(testContext, 1) { checkpoint ->
        // We create a parent, and 2 child shards
        kinesisClient.createAndGetStreamDescriptionWhenActive(3)

        createDispatcher {
            checkpoint.flag()
        }

        splitResharding()
    }

    @Test
    internal fun redis_based_merge_resharding(testContext: VertxTestContext) = asyncTest(testContext, 1) { checkpoint ->
        // We create 2 parents, and 1 child shard
        kinesisClient.createAndGetStreamDescriptionWhenActive(3)

        createDispatcher(true) {
            checkpoint.flag()
        }

        mergeResharding()
    }

    @Test
    internal fun eventbus_based_split_resharding(testContext: VertxTestContext) =
        asyncTest(testContext, 1) { checkpoint ->
            // We create a parent, and 2 child shards
            kinesisClient.createAndGetStreamDescriptionWhenActive(3)

            createDispatcher {
                checkpoint.flag()
            }

            splitResharding()
        }

    @Test
    internal fun eventbus_based_merge_resharding(testContext: VertxTestContext) =
        asyncTest(testContext, 1) { checkpoint ->
            // We create 2 parents, and 1 child shard
            kinesisClient.createAndGetStreamDescriptionWhenActive(3)

            createDispatcher(true) {
                checkpoint.flag()
            }

            mergeResharding()
        }

    private fun splitResharding() {
        val reshardingEvent =
            SplitReshardingEvent(ShardIdGenerator.generateShardId(), ShardIdGenerator.generateShardIdList(2, 1))
        eventBus.send(reshardingEvent.getNotificationAddr(), reshardingEvent)
    }

    private suspend fun mergeResharding() {
        val parentShardId = ShardIdGenerator.generateShardId()
        val adjacentParentShardId = ShardIdGenerator.generateShardId(1)
        val childShardId = ShardIdGenerator.generateShardId(2)

        val reshardingEventParentShardFinished = MergeReshardingEvent(
            parentShardId,
            childShardId
        )
        // We have to simulate that's the parent shard is flagged finished, as like by consumer verticle
        shardStatePersistenceService.saveFinishedShard(parentShardId, 10000)
        eventBus.send(reshardingEventParentShardFinished.getNotificationAddr(), reshardingEventParentShardFinished)

        // We need to send it twice, as this would singal that both parents are finished
        val reshardingEventAdjacentParentShardFinished = MergeReshardingEvent(
            adjacentParentShardId,
            childShardId
        )

        // We have to simulate that's the adjacent parent shard is flagged finished, as like by consumer verticle
        shardStatePersistenceService.saveFinishedShard(adjacentParentShardId, 10000)
        eventBus.send(
            reshardingEventParentShardFinished.getNotificationAddr(),
            reshardingEventAdjacentParentShardFinished
        )
    }

    private suspend fun createDispatcher(eventBusCommunication: Boolean = false, block: () -> Unit) {
        ReOrchestrationCmdDispatcher.create(
            vertx,
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            kinesisClient,
            shardStatePersistenceService,
            defaultTestScope,
            redisOptions,
            eventBusBaseDispatching = eventBusCommunication,
            reshardingEventHandler = block
        ).start()
    }
}
