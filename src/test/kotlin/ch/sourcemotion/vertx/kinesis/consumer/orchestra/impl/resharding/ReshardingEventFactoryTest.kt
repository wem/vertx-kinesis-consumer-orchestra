package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test

internal class ReshardingEventFactoryTest : AbstractKinesisAndRedisTest() {

    @Test
    internal fun shard_split_event(testContext: VertxTestContext) = asyncTest(testContext) {
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        val parentShard = streamDescription.shards().first()

        kinesisClient.splitShardFair(parentShard)

        val sut = ReshardingEventFactory(
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME),
            parentShard.shardIdTyped()
        )

        val reshardingEvent = sut.createReshardingEvent()
        reshardingEvent.reshardingType.shouldBe(ReshardingType.SPLIT)
        reshardingEvent.shouldBeInstanceOf<SplitReshardingEvent>()
        reshardingEvent.finishedParentShardId.shouldBe(parentShard.shardIdTyped())
        reshardingEvent.childShardIds.shouldContainAll(ShardIdGenerator.generateShardIdList(2, 1))
    }

    @Test
    internal fun shard_merged_event(testContext: VertxTestContext) = asyncTest(testContext) {
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(2)
        val parentShard = streamDescription.shards().first()
        val adjacentShard = streamDescription.shards()[1]

        kinesisClient.mergeShards(parentShard, adjacentShard)

        val parentShardReshardingFactory = ReshardingEventFactory(
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME),
            parentShard.shardIdTyped()
        )
        val parentReshardingEvent = parentShardReshardingFactory.createReshardingEvent()
        parentReshardingEvent.shouldBeInstanceOf<MergeReshardingEvent>()
        parentReshardingEvent.finishedParentShardId.shouldBe(parentShard.shardIdTyped())
        parentReshardingEvent.childShardId.shouldBe(ShardIdGenerator.generateShardId(2))

        val adjacentParentShardReshardingFactory = ReshardingEventFactory(
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME),
            adjacentShard.shardIdTyped()
        )
        val adjacentParentReshardingEvent = adjacentParentShardReshardingFactory.createReshardingEvent()
        adjacentParentReshardingEvent.shouldBeInstanceOf<MergeReshardingEvent>()
        adjacentParentReshardingEvent.finishedParentShardId.shouldBe(adjacentShard.shardIdTyped())
        adjacentParentReshardingEvent.childShardId.shouldBe(ShardIdGenerator.generateShardId(2))
    }

    @Test
    internal fun no_valid_children() {
        val parentId = ShardIdGenerator.generateShardId(0)
        val reshardingEventFactory = ReshardingEventFactory(
            mock { on { shards() } doReturn emptyList() },
            parentId
        )
        shouldThrow<VertxKinesisConsumerOrchestraException> { reshardingEventFactory.createReshardingEvent() }
    }
}
